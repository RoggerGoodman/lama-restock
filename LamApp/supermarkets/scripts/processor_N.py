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
        minimum_stock_base: per-storage presence target — units that must face the
            customer. Reduced for slow movers, never touched by the trend factor.
        minimum_stock_override: Product-level override. If set, used as-is in place of the
            base/velocity/slow-mover/deviation/expiry-rate calculation below — those all
            reflect the algorithm's own judgment, which the override exists to replace.
            Shelf-life cap and active batch-expiry-risk cap still apply on top of it, since
            those are physical facts about today's stock, not judgment calls the override
            can account for in advance.
        sigma_L: measured demand sigma over the coverage window. Replaces the legacy
            sqrt(req_stock) buff for products above SLOW_MOVER_THRESHOLD, since that
            rule assumes Poisson (variance == mean) while real dispersion is 2.67
            volume-weighted and 20-60 for pack-bought beverages. None (thin history)
            or a slow mover keeps the legacy rule unchanged.
        safety_z: standard deviations of cushion, per settore — Helper.safety_z_for.
    """
    order = 1
    req_stock = round(req_stock)
    leftover_stock = stock - req_stock

    if minimum_stock_override is not None:
        minimum_stock = minimum_stock_override
        logger.info(f"Minimum stock override = {minimum_stock} (velocity/deviation/expiry-rate adjustments skipped)")
    elif sigma_L is not None and avg_daily_sales >= Helper.SLOW_MOVER_THRESHOLD:
        # Gated at the same velocity threshold the legacy buff used, and for the
        # same reason: below it the pack size IS the safety stock. A product
        # selling 0.16/day with req_stock under 1 unit ships in packs of 8 — 50
        # days of cover — so a statistical buffer on top is meaningless, and the
        # legacy branch deliberately applied a penalty there instead of a buff.
        presence_target = minimum_stock_base

        safety = safety_z * sigma_L
        # Never hold more buffer than one coverage window of demand
        capped = min(safety, float(req_stock)) if req_stock > 0 else safety

        factor = Helper.deviation_factor(deviation_corrected)
        adjusted = capped * factor

        # Combine the two requirements in quadrature rather than by sum or by max.
        # minimum_stock is the expected residual at the end of the protection
        # interval, and it carries two independent jobs: hold z*sigma so demand
        # variance does not stock us out, and hold `presence_target` facings so the
        # shelf does not look empty.
        #   sum      -> double-counts; a steady seller with presence 5 already has
        #               ~3 sigma of cover, so adding sigma on top buys nothing
        #   max      -> the larger swallows the smaller, leaving minimum_stock flat
        #               at `presence` for every product with sigma < presence. That
        #               dead zone widens with the storage's presence setting, so a
        #               store configured at 20 would have sigma inert almost
        #               everywhere.
        #   quadrature -> collapses to the larger when either dominates (both ends
        #               behave like max), gives 1.41x when they are comparable, and
        #               is strictly increasing in sigma at every presence value, so
        #               there is no dead zone. It also reproduces the legacy buff
        #               through the mid band.
        minimum_stock = round(math.sqrt(presence_target ** 2 + adjusted ** 2))
        logger.info(
            f"Safety stock: z={safety_z} x sigma_L={sigma_L:.1f} = {safety:.1f}"
            + (f" (capped to req_stock={req_stock})" if capped < safety else "")
            + (f" x deviation {deviation_corrected:+.0f}% = {adjusted:.1f}" if factor != 1.0 else "")
            + f" -> minimum_stock = hypot(presence {presence_target}, safety {adjusted:.1f}) = {minimum_stock}"
        )

        # No on-sale bonus here on purpose: the measured promo lift has already
        # scaled req_stock upstream, so a second multiplier would double-count.
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
        # Only raise the post-nerf floor if the shelf life supports at least 1 full unit
        # of buffer (msb >= 1). Fractional capacity (0 < msb < 1) means ordering 1 extra
        # would already exceed what can sell before expiry.
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