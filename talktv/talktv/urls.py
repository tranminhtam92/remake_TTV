"""talktv URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url, include
from django.contrib import admin

urlpatterns = [
    url(r'^admin/', include(admin.site.urls)),
    url(r'^event/', include('log_event.urls')),
    url(r'^', include('log_report.urls')),
]

from django.http import JsonResponse


def handle_500(request):
    return JsonResponse(
        {
            'status_code': '500',
            'message': 'Please contact me (Pham Huu Danh | danhph3@vng.com.vn | 0167.483.7720) if this is urgent case!'
        },
        status=500
    )


handler500 = handle_500
