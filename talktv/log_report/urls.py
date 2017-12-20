from django.conf.urls import url

from log_report import views

urlpatterns = [
    url(r'^$', views.IndexView.as_view(), name='report_index'),
    url(r'report/?$', views.ReportView.as_view(), name='report_data'),
    url(r'export/?$', views.ExportView.as_view(), name='report_export'),
    url(r'col/(?P<col>[\w_]+)/?$', views.ReportPerColView.as_view(), name='report_data_per_col'),
]
