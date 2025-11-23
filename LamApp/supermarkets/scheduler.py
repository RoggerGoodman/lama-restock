from apscheduler.schedulers.background import BackgroundScheduler
import datetime
from .models import RestockSchedule
import atexit

def start():
    scheduler = BackgroundScheduler()
    scheduler.add_job(check_and_run_schedules, 'interval', minutes=1)
    scheduler.start()

    # Make sure the scheduler stops when Django stops
    atexit.register(lambda: scheduler.shutdown())

def check_and_run_schedules():
    now = datetime.datetime.now()
    current_day = now.strftime('%A').lower()  # 'monday', 'tuesday', etc.
    current_time = now.time().replace(second=0, microsecond=0)

    all_schedules = RestockSchedule.objects.all()

    for schedule in all_schedules:
        # Get the day's value (off/early/late)
        day_status = getattr(schedule, current_day)

        if day_status == 'off':
            continue  # Skip if no restock for this day

        # Calculate expected time for this schedule today
        scheduled_time = schedule.restock_time

        # Match the current time exactly (rounded to the minute)
        if scheduled_time == current_time:
            print(f"Running restock for schedule: {schedule}")
            #run_restock_check(schedule) #TODO make it do the thing