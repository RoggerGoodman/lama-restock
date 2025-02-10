from django.contrib import admin
from .models import Supermarket, Category, RestockSchedule, Blacklist, BlacklistEntry

admin.site.register(Supermarket)
admin.site.register(Category)
admin.site.register(RestockSchedule)
admin.site.register(Blacklist)
admin.site.register(BlacklistEntry)