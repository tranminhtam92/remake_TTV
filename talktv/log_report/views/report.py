from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from log_report.serializers import RequestSerializer
from log_storage.support_models import StreamerInfo
from talktv.constants import ReportType


class ReportView(APIView):
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
        #     for report in ordered_reports:
        #         del report['_id']
        #         report['streamer_name'] = streamers.get(report['streamer_uin'], report['streamer_uin'])
        #         report['total_view_length'] = round(report['total_view_length'], 2)
        #
        # elif report_type == ReportType.COMMENT:
        #     ordered_reports = self.report_total_comments.get_reports(dt_from, dt_to, room_id)
        #     for report in ordered_reports:
        #         del report['_id']
        #
        # elif report_type in (ReportType.RANK_1_DAY,
        #                      ReportType.RANK_7_DAY,
        #                      ReportType.RANK_30_DAY):
        #     ordered_reports = self.rank_report.get_reports(report_type, dt_from, dt_to, is_game)
        #     for report in ordered_reports:
        #         del report['_id']
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
        #     for report in ordered_reports:
        #         del report['_id']
        #         report['total_view_length'] = round(report['total_view_length'], 2)
        #
        # return Response(data={
        #     'length': len(ordered_reports),
        #     'query_params': serializer.validated_data,
        #     'results': ordered_reports
        # })


class ReportPerColView(APIView):
    def get(self, request, col):
        return Response()
        # if col == 'active_users':
        #     report_col = self.report_active_users
        # elif col == 'active_users_3min':
        #     report_col = self.report_active_users_3min
        # elif col == 'total_view_length':
        #     report_col = self.report_total_view_length
        # else:
        #     return Response(status=status.HTTP_400_BAD_REQUEST)
        #
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
        # # room_id = serializer.validated_data['room_id']
        # # streamer_uins = serializer.validated_data['streamer_uin']
        #
        # reports = report_col.get_reports(report_type, dt_from, dt_to,
        #                                  platform, category, sub_category)
        #
        # for report in reports:
        #     del report['_id']
        #
        # return Response(data={
        #     'length': len(reports),
        #     'query_params': serializer.validated_data,
        #     'results': reports
        # })
