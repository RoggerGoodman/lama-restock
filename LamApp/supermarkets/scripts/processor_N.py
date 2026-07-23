# LamApp/supermarkets/scripts/processor_N.py
import math
import logging

from .helpers import Helper

# Use Django's logging system
logger = logging.getLogger(__name__)


def process_N_sales(package_size, deviation_corrected, avg_daily_sales,
                   req_stock, stock, discount=None, minimum_stock_base=None, minimum_stock_override=None,
                   expiry_factor=None, shelf_life_days=None, batch_expiry_factor=None,
                   sigma_L=None, safety_z=1.0):
    """
    Process N category sales and calculate order quantity.

    minimum_stock = presence (merchandising facings) + safety (z * sigma_L).

    Args:
        minimum_stock_base: per-storage presence target (facings). Reduced for slow
            movers, never touched by the trend factor.
        minimum_stock_override: product-level presence target. Replaces the algorithm's
            judgement terms (velocity ladder, deviation trend, expiry rate); shelf-life
            and batch-expiry caps still apply, being facts about today's stock.
        sigma_L: measured demand sigma over the coverage window. Replaces the legacy
            sqrt(req_stock) buff above SLOW_MOVER_THRESHOLD (that buff assumes Poisson;
            real dispersion is far higher). None or a slow mover keeps the legacy rule.
        safety_z: standard deviations of cushion, per settore — Helper.safety_z_for.
    """
    order = 1
    req_stock = round(req_stock)
    leftover_stock = stock - req_stock

    # An override sets the presence target only — it says nothing about that
    # product's demand volatility, so sigma still applies.
    has_override = minimum_stock_override is not None
    presence_target = minimum_stock_override if has_override else minimum_stock_base

    if sigma_L is not None and avg_daily_sales >= Helper.SLOW_MOVER_THRESHOLD:
        # Gated at the legacy buff's velocity threshold: below it the pack size is
        # already many weeks of cover, so a statistical buffer adds nothing.
        safety = safety_z * sigma_L
        # Never hold more buffer than one coverage window of demand
        capped = min(safety, float(req_stock)) if req_stock > 0 else safety

        # The trend is judgement, so an override suppresses it
        factor = 1.0 if has_override else Helper.deviation_factor(deviation_corrected)
        adjusted = capped * factor

        # Quadrature, not sum (double-counts) or max (leaves sigma inert whenever
        # it sits below presence): behaves like max at both extremes, 1.41x when
        # the two terms are comparable, and always increasing in sigma.
        minimum_stock = round(math.sqrt(presence_target ** 2 + adjusted ** 2))
        logger.info(
            f"Safety stock: z={safety_z} x sigma_L={sigma_L:.1f} = {safety:.1f}"
            + (f" (capped to req_stock={req_stock})" if capped < safety else "")
            + (f" x deviation {deviation_corrected:+.0f}% = {adjusted:.1f}" if factor != 1.0 else "")
            + f" -> minimum_stock = hypot({'override' if has_override else 'presence'} {presence_target},"
            + f" safety {adjusted:.1f}) = {minimum_stock}"
        )

        # No on-sale bonus: promo lift already scaled req_stock upstream.
    elif has_override:
        # No usable sigma (thin history or slow mover) — the override stands alone.
        minimum_stock = presence_target
        logger.info(f"Minimum stock override = {minimum_stock} (no sigma available; judgement terms skipped)")
    else:
        presence_target = minimum_stock_base  # always-on-the-shelf baseline, storage-configured

        if avg_daily_sales >= 0.6:
            buff = max(0, round(math.sqrt(max(0, req_stock - 1))) - 1)
            demand_margin = buff
            if discount is not None:
                demand_margin += (buff * 2)
                logger.info(
                    f"Velocity buff: avg_daily_sales={avg_daily_sales:.2f} -> "
                    f"+{buff}, +{buff * 2} on-sale bonus (total +{buff * 3})"
                )
            else:
                logger.info(f"Velocity buff: avg_daily_sales={avg_daily_sales:.2f} -> +{buff}")
        else:
            demand_margin = -Helper.slow_mover_reduction(avg_daily_sales)
            logger.info(f"Slow-mover reduction: avg_daily_sales={avg_daily_sales:.2f} -> {demand_margin}")

        minimum_stock = presence_target + demand_margin
        logger.info(f"Baseline={presence_target}, demand margin={demand_margin:+d} -> minimum_stock={minimum_stock}")

        pre_deviation = minimum_stock
        factor = Helper.deviation_factor(deviation_corrected)
        if factor != 1.0:
            # Round toward the unadjusted value: floor when buffing up, ceil when
            # cutting. Applies slightly less of the trend than the raw multiplier.
            scaled = minimum_stock * factor
            minimum_stock = math.floor(scaled) if factor > 1.0 else math.ceil(scaled)
        if minimum_stock != pre_deviation:
            logger.info(f"Deviation adjustment: deviation={deviation_corrected:.1f}% -> minimum_stock {pre_deviation} -> {minimum_stock}")

        pre_floor = minimum_stock
        minimum_stock = max(1, round(minimum_stock))
        if minimum_stock != pre_floor:
            logger.info(f"Floor clamp: minimum_stock raised {pre_floor} -> {minimum_stock} (floor=1)")

    if expiry_factor is not None and minimum_stock_override is None:
        pre_expiry = minimum_stock
        minimum_stock = math.floor(minimum_stock * expiry_factor)
        logger.info(f"Expiry factor {expiry_factor} applied -> minimum_stock {pre_expiry} -> {minimum_stock}")

    shelf_life_has_buffer = False
    if shelf_life_days is not None:
        max_safe_buffer = shelf_life_days * avg_daily_sales - req_stock
        pre_shelf_life = minimum_stock
        minimum_stock = min(minimum_stock, max(0, int(max_safe_buffer)))
        # Floor of 1 only if shelf life supports a full unit of buffer; fractional
        # capacity means the extra unit would expire unsold.
        shelf_life_has_buffer = max_safe_buffer >= 1
        if minimum_stock != pre_shelf_life:
            logger.info(
                f"Shelf-life cap: {shelf_life_days}d shelf life, max_safe_buffer={max_safe_buffer:.1f} "
                f"-> minimum_stock capped {pre_shelf_life} -> {minimum_stock}"
            )

    if batch_expiry_factor and minimum_stock > 1:
        logger.info(f"Batch expiry risk detected -> minimum_stock capped from {minimum_stock} to 1")
        minimum_stock = 1

    minimum_stock = max(1 if shelf_life_has_buffer else 0, minimum_stock)
    logger.info(f"Minimum Stock (final) = {minimum_stock}")

    raw_order = (req_stock + minimum_stock - stock) / package_size
    order = raw_order
    if order >= 0:
        tollerance_threshold = min(0.5, minimum_stock/package_size)
        decimal_part = order % 1
        if batch_expiry_factor:
            order = math.floor(order)
        elif decimal_part <= tollerance_threshold:
            order = math.floor(order)
        else:
            order = math.ceil(order)

        if order >= 1:
            logger.info(
                f"Order decision: {order} package(s) (raw={raw_order:.2f}) — formula "
                f"(req_stock={req_stock} + minimum_stock={minimum_stock} - stock={stock}) / package_size={package_size}"
            )
            return order, 1, True, discount

    if leftover_stock <= minimum_stock:
        order = 1
        logger.info(f"Order decision: forced 1 package — leftover_stock={leftover_stock} < minimum_stock={minimum_stock}")
        return order, 2, True, discount

    if discount != None and stock <= package_size*0.2 or stock <= package_size*0.1:
        order = 1
        threshold_desc = "20% of package (on sale)" if discount is not None else "10% of package"
        logger.info(f"Order decision: forced 1 package — stock={stock} at or below {threshold_desc} (package_size={package_size})")
        return order, 3, True, discount

    logger.info(
        f"No order: leftover_stock={leftover_stock} >= minimum_stock={minimum_stock} and stock={stock} not critically low"
    )
    return None, 0, False, discount