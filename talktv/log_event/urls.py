from django.conf.urls import url

from log_event import views

urlpatterns = [
    url(r'gift/?$', views.GiftDataView.as_view(), name='event_gift_data'),
    url(r'login/?$', views.LoginView.as_view(), name='event_login'),
    url(r'logout/?$', views.LogoutView.as_view(), name='event_logout'),
    url(r'now/?$', views.DateTimeView.as_view(), name='event_date_time'),
]
