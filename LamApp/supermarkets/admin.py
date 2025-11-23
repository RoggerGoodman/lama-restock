from django.contrib import admin
from .models import Supermarket, Storage, RestockSchedule, Blacklist, BlacklistEntry

admin.site.register(Supermarket)
admin.site.register(Storage)
admin.site.register(RestockSchedule)
admin.site.register(Blacklist)
admin.site.register(BlacklistEntry)