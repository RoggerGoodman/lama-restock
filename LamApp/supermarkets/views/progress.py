"""Background task progress pages and polling."""

from django.shortcuts import render, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from LamApp.celery import app as celery_app
import logging

from ..models import Storage, RestockLog

logger = logging.getLogger(__name__)


@login_required
def task_progress_view(request, task_id, storage_id=None):
    """
    Generic progress view for ANY Celery task.
    Shows real-time progress and auto-redirects when complete.
    """
    from celery.result import AsyncResult
    
    # FIX: Bind to our Celery app instance
    task = AsyncResult(task_id, app=celery_app)
    storage = None
    
    if storage_id:
        storage = get_object_or_404(
            Storage,
            id=storage_id,
            supermarket__owner=request.user
        )
    
    context = {
        'task_id': task_id,
        'task_state': task.state,
        'storage': storage
    }
    
    return render(request, 'tasks/progress.html', context)


@login_required
def task_status_ajax_view(request, task_id):
    """
    FIXED: Better handling of different task result formats.
    Always generates a redirect_url or message for frontend.
    """
    from celery.result import AsyncResult

    task = AsyncResult(task_id, app=celery_app)

    # Debug logging to diagnose infinite loading issues
    logger.debug(f"Task {task_id}: state={task.state}, ready={task.ready()}, info={task.info}")

    # ✅ FIX: Check both task.ready() AND explicit SUCCESS/FAILURE states
    # This fixes cases where task.ready() returns False incorrectly
    is_complete = task.ready() or task.state in ['SUCCESS', 'FAILURE']

    response_data = {
        'state': task.state,
        'ready': is_complete,  # Use our corrected completion check
        'task_id': task_id,
    }

    if is_complete:
        if task.successful():
            result = task.result
            response_data['success'] = True
            
            # ✅ FIXED: Always ensure we have either redirect_url or message
            if isinstance(result, dict):
                response_data['result'] = result
                
                # Extract message if available
                message = result.get('message', 'Operation completed successfully')
                response_data['message'] = message
                
                # Determine redirect URL based on result content
                if 'log_id' in result:
                    # Restock operation
                    response_data['redirect_url'] = f"/logs/{result['log_id']}/"
                
                elif 'storage_id' in result:
                    # Storage operation (list update, stats update, etc.)
                    response_data['redirect_url'] = f"/storages/{result['storage_id']}/"
                
                elif 'synced' in result:
                    # Storage sync operation
                    response_data['redirect_url'] = f"/supermarkets/{result['supermarket_id']}/edit/"

                elif 'products_added' in result:
                    # Add products operation
                    response_data['redirect_url'] = "/inventory/"
                
                elif 'verified' in result or 'assigned' in result:
                    # Inventory verification or cluster assignment
                    response_data['redirect_url'] = "/inventory/"
                
                else:
                    # Unknown format - default to dashboard
                    response_data['redirect_url'] = "/dashboard/"
            
            else:
                # Non-dict result (shouldn't happen, but handle it)
                response_data['message'] = str(result) if result else 'Operation completed'
                response_data['redirect_url'] = "/dashboard/"
        
        else:
            # Task failed
            response_data['success'] = False
            error_info = task.info
            
            if isinstance(error_info, Exception):
                response_data['error'] = str(error_info)
            elif isinstance(error_info, dict):
                response_data['error'] = error_info.get('exc_message', str(error_info))
            else:
                response_data['error'] = str(error_info) if error_info else 'Unknown error'
    
    else:
        # Task still running - extract progress info
        if isinstance(task.info, dict):
            response_data['progress'] = task.info.get('progress', 0)
            response_data['status_message'] = task.info.get('status', 'Processing...')
        else:
            response_data['progress'] = 0
            response_data['status_message'] = 'Processing...'

        # ✅ FIX: If task has been PENDING for too long, check if result exists anyway
        # This handles edge cases where Celery doesn't update state properly
        if task.state == 'PENDING':
            try:
                # Try to get the result anyway - if it exists, the task is actually done
                result = task.result
                if result is not None:
                    logger.warning(f"Task {task_id} stuck in PENDING but has result. Marking as complete.")
                    response_data['ready'] = True
                    response_data['success'] = True
                    response_data['result'] = result

                    # Extract redirect URL from result
                    if isinstance(result, dict):
                        response_data['message'] = result.get('message', 'Operation completed')

                        if 'log_id' in result:
                            response_data['redirect_url'] = f"/logs/{result['log_id']}/"
                        elif 'storage_id' in result:
                            response_data['redirect_url'] = f"/storages/{result['storage_id']}/"
                        else:
                            response_data['redirect_url'] = "/dashboard/"
                    else:
                        response_data['message'] = 'Operation completed'
                        response_data['redirect_url'] = "/dashboard/"
            except Exception as e:
                logger.debug(f"Task {task_id} is genuinely pending: {e}")

    logger.debug(f"Task {task_id} response: ready={response_data.get('ready')}, state={response_data.get('state')}")
    return JsonResponse(response_data)


@login_required
def restock_task_progress_view(request, task_id):
    """
    Specialized progress view for restock operations.
    Uses RestockLog for detailed checkpoint tracking.
    """
    from celery.result import AsyncResult
    
    # FIX: Bind to our Celery app instance
    task = AsyncResult(task_id, app=celery_app)
    
    # Try to find log from task result
    log = None
    if task.ready() and task.successful():
        result = task.result
        if isinstance(result, dict) and 'log_id' in result:
            try:
                log = RestockLog.objects.get(
                    id=result['log_id'],
                    storage__supermarket__owner=request.user
                )
            except RestockLog.DoesNotExist:
                pass
    
    context = {
        'task_id': task_id,
        'task_state': task.state,
        'log': log
    }
    
    return render(request, 'storages/restock_task_progress.html', context)
