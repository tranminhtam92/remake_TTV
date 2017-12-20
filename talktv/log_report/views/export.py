import io

import xlsxwriter
from django.http import HttpResponse
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from log_report.serializers import RequestSerializer
from log_storage.support_models import StreamerInfo
from talktv.constants import ReportType, PLATFORM_IDS, CATEGORY_IDS, SUB_CATEGORY_IDS, RANK_CHOICES


class ExportView(APIView):
    streamer_info = StreamerInfo.objects

    @staticmethod
    def update_kpi_reports(reports, raw_reports):
        for raw_report in raw_reports:
            k = '_'.join([raw_report['report_date'].strftime('%Y%m%d_%H%M%S%f'),
                          str(raw_report['room_id']),
                          str(raw_report['streamer_uin']),
                          raw_report['stream_start'].strftime('%Y%m%d_%H%M%S%f'),
                          raw_report['stream_stop'].strftime('%Y%m%d_%H%M%S%f'), ])
            if k not in reports:
                reports[k] = {
                    'active_users': 0,
                    'active_users_3min': 0,
                    'total_view_length': 0
                }
            reports[k].update(raw_report)

    @staticmethod
    def update_reports(reports, raw_reports):
        for raw_report in raw_reports:
            k = '_'.join([raw_report['report_date'].strftime('%Y%m%d_%H%M%S%f'),
                          str(raw_report['platform']),
                          str(raw_report['category']),
                          str(raw_report['sub_category']), ])
            if k not in reports:
                reports[k] = {
                    'active_users': 0,
                    'active_users_3min': 0,
                    'total_view_length': 0
                }
            reports[k].update(raw_report)

    @staticmethod
    def get_value_alias(choices, value):
        if value is None:
            return None
        for i in choices:
            if i[0] == value:
                return i[1]
        return None

    def get(self, request):
        return Response()
        # serializer = RequestSerializer(data=self.request.query_params)
        # if not serializer.is_valid():
        #     return Response(status=status.HTTP_400_BAD_REQUEST, data=serializer.errors)
        #
        # dt_from = serializer.validated_data['dt_from']
        # dt_to = serializer.validated_data['dt_to']
        # report_type = serializer.validated_data['report_type']
        # platform = serializer.validated_data['platform']
        # category = serializer.validated_data['category']
        # sub_category = serializer.validated_data['sub_category']
        # room_id = serializer.validated_data['room_id']
        # streamer_uin = serializer.validated_data['streamer_uin']
        # is_game = serializer.validated_data['is_game']
        #
        # output = io.BytesIO()
        #
        # workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        # dt_format = workbook.add_format({'num_format': 'mm-dd hh:mm'})
        # worksheet = workbook.add_worksheet()
        #
        # if report_type == ReportType.STREAMING:
        #     reports = {}
        #     raw_reports = self.report_active_users.get_kpi_reports(dt_from, dt_to, room_id, streamer_uin)
        #     self.update_kpi_reports(reports, raw_reports)
        #
        #     raw_reports = self.report_active_users_3min.get_kpi_reports(dt_from, dt_to, room_id, streamer_uin)
        #     self.update_kpi_reports(reports, raw_reports)
        #
        #     raw_reports = self.report_total_view_length.get_kpi_reports(dt_from, dt_to, room_id, streamer_uin)
        #     self.update_kpi_reports(reports, raw_reports)
        #
        #     ordered_reports = reports.values()
        #     ordered_reports.sort(key=lambda r: (r['report_date'], -r['room_id'], r['stream_start']), reverse=True)
        #
        #     streamer_info = list(self.streamer_info.all())
        #     streamers = {streamer.uin: streamer.name for streamer in streamer_info}
        #
        #     worksheet.write_row(0, 0, ['Date', 'Room', 'Streamer Id', 'Streamer Name',
        #                                'Start Time', 'Stop time', 'A1', 'A1 (3min)', 'View Length'])
        #     for k, report in enumerate(ordered_reports):
        #         report['streamer_name'] = streamers.get(report['streamer_uin'], unicode(report['streamer_uin']))
        #         worksheet.write(k + 1, 0, report['report_date'], dt_format)
        #         worksheet.write(k + 1, 1, report['room_id'])
        #         worksheet.write(k + 1, 2, report['streamer_uin'])
        #         worksheet.write(k + 1, 3, report['streamer_name'])
        #         worksheet.write(k + 1, 4, report['stream_start'], dt_format)
        #         worksheet.write(k + 1, 5, report['stream_stop'], dt_format)
        #         worksheet.write(k + 1, 6, report.get('active_users', 0))
        #         worksheet.write(k + 1, 7, report.get('active_users_3min', 0))
        #         worksheet.write(k + 1, 8, report.get('total_view_length', 0))
        #
        # elif report_type == ReportType.COMMENT:
        #     ordered_reports = self.report_total_comments.get_reports(dt_from, dt_to, room_id)
        #     worksheet.write_row(0, 0, ['Date', 'Room', 'Total Comments', ])
        #     for k, report in enumerate(ordered_reports):
        #         worksheet.write(k + 1, 0, report['report_date'], dt_format)
        #         worksheet.write(k + 1, 1, report['room_id'])
        #         worksheet.write(k + 1, 2, report['total_comments'])
        #
        # elif report_type in (ReportType.RANK_1_DAY,
        #                      ReportType.RANK_7_DAY,
        #                      ReportType.RANK_30_DAY):
        #     ordered_reports = self.rank_report.get_reports(report_type, dt_from, dt_to, is_game)
        #     worksheet.write_row(0, 0, ['Date', 'Rank', 'A1', 'A1 (3min)', 'View Length', ])
        #     for k, report in enumerate(ordered_reports):
        #         worksheet.write(k + 1, 0, report['report_date'], dt_format)
        #         worksheet.write(k + 1, 1, self.get_value_alias(RANK_CHOICES, report['rank']))
        #         worksheet.write(k + 1, 2, report.get('active_users', 0))
        #         worksheet.write(k + 1, 3, report.get('active_users_3min', 0))
        #         worksheet.write(k + 1, 4, report.get('total_view_length', 0))
        #
        # else:
        #     reports = {}
        #     raw_reports = self.report_active_users.get_reports(report_type, dt_from, dt_to, platform, category,
        #                                                        sub_category, room_id, streamer_uin)
        #     self.update_reports(reports, raw_reports)
        #
        #     raw_reports = self.report_active_users_3min.get_reports(report_type, dt_from, dt_to, platform, category,
        #                                                             sub_category, room_id, streamer_uin)
        #     self.update_reports(reports, raw_reports)
        #
        #     raw_reports = self.report_total_view_length.get_reports(report_type, dt_from, dt_to, platform, category,
        #                                                             sub_category, room_id, streamer_uin)
        #     self.update_reports(reports, raw_reports)
        #
        #     ordered_reports = reports.values()
        #     ordered_reports.sort(key=lambda r: r['report_date'], reverse=True)
        #
        #     worksheet.write_row(0, 0, ['Date', 'Platform', 'Category', 'Sub Category',
        #                                'A1', 'A1 (3min)', 'View Length'])
        #     for k, report in enumerate(ordered_reports):
        #         worksheet.write(k + 1, 0, report['report_date'], dt_format)
        #         worksheet.write(k + 1, 1, self.get_value_alias(PLATFORM_IDS, report['platform']))
        #         worksheet.write(k + 1, 2, self.get_value_alias(CATEGORY_IDS, report['category']))
        #         worksheet.write(k + 1, 3, self.get_value_alias(SUB_CATEGORY_IDS, report['sub_category']))
        #         worksheet.write(k + 1, 4, report.get('active_users', 0))
        #         worksheet.write(k + 1, 5, report.get('active_users_3min', 0))
        #         worksheet.write(k + 1, 6, report.get('total_view_length', 0))
        #
        # workbook.close()
        # output.seek(0)
        #
        # response = HttpResponse(output.read(),
        #                         content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        # response['Content-Disposition'] = 'attachment; filename="report.xlsx"'
        # return response
