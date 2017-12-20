import csv
import os
from datetime import datetime

import xlsxwriter
from dateutil import parser
from dateutil.relativedelta import relativedelta
from django.conf import settings

from log_report.collections import ActiveUsers3MinReportCollection
from log_report.collections import ActiveUsersReportCollection
from log_report.collections import TotalCommentsReportCollection
from log_report.collections import TotalViewLengthReportCollection
from log_storage.collections import ChatLogCollection
from talktv.constants import ReportType, Platform
from talktv.custom import CustomBaseCommand


class Command(CustomBaseCommand):
    help = 'Export KPI reports'
    report_active_users = ActiveUsersReportCollection()
    report_active_users_3min = ActiveUsers3MinReportCollection()
    report_total_view_length = TotalViewLengthReportCollection()
    chat_log = ChatLogCollection()

    # Set up arguments

    def add_arguments(self, parser):
        parser.add_argument(
            '--from',
            dest='datetime',
            default=None,
            type=str,
            help='start time by datetime (e.g. 2017-06-23 00:00:00)',
        )
        parser.add_argument(
            '--to',
            dest='to_datetime',
            default=None,
            type=str,
            help='stop time by datetime (e.g. 2017-06-23 00:00:00)',
        )

    def read_arguments(self, options):
        """
        Read arguments from `self.options` and store them to variables
        :param options: set by function `add_arguments`
        :return: `dt_from` and `dt_to` to specific the name of log files
        """
        if options['datetime']:
            dt_from = parser.parse(options['datetime'])
        else:
            dt_from = datetime.now() - relativedelta(months=1)
        dt_from = dt_from.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        if options['to_datetime']:
            dt_to = parser.parse(options['to_datetime'])
        else:
            dt_to = dt_from
        dt_to = dt_to.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        return dt_from, dt_to

    @staticmethod
    def update_reports(reports, raw_reports):
        for raw_report in raw_reports:
            k = '_'.join([raw_report['report_date'].strftime('%Y%m%d'),
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

    def kpi_report(self, dt_from, dt_to):
        reports = {}
        raw_reports = ActiveUsersReportCollection().get_kpi_reports(dt_from, dt_to)
        self.update_reports(reports, raw_reports)

        raw_reports = ActiveUsers3MinReportCollection().get_kpi_reports(dt_from, dt_to)
        self.update_reports(reports, raw_reports)

        raw_reports = TotalViewLengthReportCollection().get_kpi_reports(dt_from, dt_to)
        self.update_reports(reports, raw_reports)

        file_name = 'kpi_report_{}_{}.csv'.format(
            dt_from.strftime('%Y%m%d'),
            dt_to.strftime('%Y%m%d')
        )

        with open(os.path.join(settings.OUTPUT_DIR, file_name), 'w+b') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['report_date', 'room_id', 'streamer_uin', 'stream_start', 'stream_stop',
                                 'active_users', 'active_users_3min', 'total_view_length'])
            ordered_reports = reports.values()
            ordered_reports.sort(key=lambda r: (r['report_date'], r['room_id'], r['stream_start']))
            for report in ordered_reports:
                csv_writer.writerow([
                    report['report_date'],
                    report['room_id'],
                    report['streamer_uin'],
                    report['stream_start'],
                    report['stream_stop'],
                    report.get('active_users', 0),
                    report.get('active_users_3min', 0),
                    report.get('total_view_length', 0),
                ])

    @staticmethod
    def comment_report(dt_from, dt_to):
        log = ChatLogCollection()
        report = TotalCommentsReportCollection()

        file_name = 'kpi_comments_{}_{}.csv'.format(
            dt_from.strftime('%Y%m%d'),
            dt_to.strftime('%Y%m%d')
        )

        result = log.get_total_comments_by_room(dt_from=dt_from, dt_to=dt_to)
        result.sort(key=lambda i: (i['report_date'], i['room_id']))

        with open(os.path.join(settings.OUTPUT_DIR, file_name), 'w+b') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['report_date', 'room_id', 'total_comments'])
            for data in result:
                csv_writer.writerow([
                    data['report_date'],
                    data['room_id'],
                    data['total_comments'],
                ])

    def monthly_report(self, dt_from, dt_to):
        print dt_from, dt_to
        streamers = [1907608, 14438433, 16375719, 17073481, 49326632, 73138727, 82240385, 121738431, 127306859,
                     127636112, 216869959, 230698170, 240577306, 251962543, 252064988, 259582051, 272014948, 281138313,
                     286858180, 296247186, 301040144, 302579357, 305561959, 306580173, 311049624, 312069018, 313915284,
                     319791474, 321772161, 324452859, 325240457, 326918172, 330250336, 337924751, 342034910, 342595136,
                     353266849, 363584828, 364383339, 365418352, 365817145, 371868729, 375708498, 375856534, 378843712,
                     379056418, 380346487, 389625993, 397011493, 402414249, 403711124, 404483621, 404642667, 406314619,
                     409805414, 411862677, 413229591, 413508315, 414039048, 419351875, 420467226, 427346297, 431939086,
                     433589089, 436223623, 438641633, 440962338, 440992527, 443418061, 443715858, 447449594, 448914002,
                     448960434, 449013065, 449210331, 449891607, 449963415, 450218756, 450387907, 450425623, 450793877,
                     450809845, 451026328, 451252819, 451903519, 451927021, 452854740, 452854808, 452983392, 453014912,
                     453715277, 454922010, 455615272, 455634770, 455634906, 455704812, 455706678, 455712347, 455715605,
                     455742049, 455828162, 455829154, 455829752, 456003514, 456482837, 456497107, 456610449, 456754937,
                     456940234, 456940419, 457009134, 457009196, 457009252, 457009491, 457009545, 457009616, 457010147,
                     457010271, 457010329, 457010400, 457272816, 457318641, 457318665, 457335823, 457336070, 457463178,
                     457567279, 457648447, 457648502, 457721705, 457795369, 457950036, 457997602, 457997667, 457997691,
                     458143867, 458146618, 458146674, 458152259, 458159582, 458180576, 458196283, 458309963, 458322038,
                     458527954, 458610822, 458617244, 458617575, 458617655, 458632711, 458632888, 458633171, 458633415,
                     458633512, 458633598, 458633667, 458633714, 458633868, 458634046, 458634107, 458634179, 458634434,
                     458687097, 458783130, 458795540, 458891691, 458971941, 458972044, 458972076, 458972113, 459393219,
                     459393771, 459631244, 459738235, 459738398, 459738882, 459759858, 459764557, 459770872, 459861056,
                     459861299, 459863210, 460376462, 460518857, 460645206, 460726804, 460754824, 460851592, 460981536,
                     461601273, 461603034, 461603886, 461650598, 461658178, 461658234, 461658277, 461658346, 461684513,
                     461823379, 461939874, 461947903, 462344574, 462544336, 462561676, 462561781, 462564120, 462564380,
                     462602818, 462713063, 462718742, 462718824, 463346177, 463347119, 463426835, 463426870, 463426938,
                     463427016, 463427767, 463428831, 463429004, 463429114, 463494279, 463494412, 463528933, 464244613,
                     464244698, 464244777, 464295197, 465073379, 465073665, 465107147, 465107426, 465146602, 465146898,
                     465382806, 465767495, 465920508, 465920728, 465921392, 465935321, 465935748, 465935852, 465935899,
                     466451193, 466830776, 466831212, 467391616, 467391804, 467734182, 467795641, 467893611, 900010327,
                     900011759, 900011918, 900024536, 900024537, 900024542, 900024933, 900024950, 900024969, 900025132,
                     900026786, 900030538, 900046233, 900048871, 900048892, 900049647, 900056569, 900056793, 900062014,
                     900062988, 900075706, 900075708, 900076157, 900076160, 900076251, 900085513, 900085515, 900085517,
                     900096008, 900096014, 900096019, 900096033, 900096110, 900118428, 900118435, 900118439, 900131891,
                     900131893, 900131901, 900131903, 900132133, 900132136, 900132138, 900132142, 900132145, 900132146,
                     900133058, 900133061, 900146853, 900167864, 900167960, 900167964, 900167976, 900171393, 900175534,
                     900175542, 900175573, 900205385, 900205393, 900205395, 900205401, 900205424, 900205428, 900218020,
                     900222710, 900222714, 900223762, 900224214, 900251229, 900255470, 900322152, 900322158, 900323756,
                     900327099, 900333176, 900351939, 900352359, 900352409, 900353457, 12553636, 391606722, 356646882,
                     307796593, 302887227, 383146079, 429386467, 432014010, 436814857, 436817703, 436820124, 22253914,
                     130002290, 342738834, 400525556, 401671049, 429637531, 429637883, 430322496, 435753976, 435758819,
                     435760244, 459503419, 459509600, 445705365, 448541974, 383761416, 420033467, 423373122, 428413252,
                     303549784, 322327012, 322896358, 324931121, 351875776, 375507546, 392185475, 415889639, 281076585,
                     433960625, 428961189, 428963657, 428965807, 428971086, 428974225, 428981963, 431258113, 431259988,
                     431261637, 431262643, 432013424, 310115806, 318699663, 328943519, 328951054, 328955890, 328959482,
                     328962356, 331056258, 332163743, 334356285, 347612339, 359681621, 399171885, 418321948, 432061578,
                     442811839, 455149589, 381916169, 441065716, 390690516, 421941221, 434762357, 434762681, 311049624,
                     343822138, 396549858, 333745756, 376502593, 392147887, 392245927, 57167564, 139872000, 208732183,
                     301971202, 310106614, 430772967, 433492603, 434048491, 445318098, 452043113, 457410819, 457726739,
                     457746820, 458121172, 458127275, 459207038, 459207250, 459207807, 460690446, 461870989, 462151366,
                     430713222, 430788202, 96328901, 216100094, 313221721, 384825011, 415273677, 427425395, 430124505,
                     430749609, 460262320, 461200891, 462535031, 211898317, 266073785, 413108930, 414473859, 428363929,
                     428364179, 414660800, 432816048, 432855983, 432879705, 449061672, 308260224, 353569028, 417320880,
                     3024244, 349173470, 349176819, 349641505, 358403727, 376872217, 392223730, 2186907, 358385813,
                     386744934, 434852066, 374357699, 457292338, 457292707, 457292776, 459268135, 391328820, 434034236,
                     434034973, 434035412, 457016098, 457019844, 288871099, 442347944, 428500066, 431942419, 433184652,
                     433223288, 433384169, 438636093, 450380870, 450629859, 453018347, 453026211, 219961619, 419898501,
                     284386342, 459522751, 377058993, 412666564, 22043698, 420571725, 430649525, 433905778, 435698718,
                     449547054, 454005762, 454876716, 456853767, 458165444, 460883142, 409856289, 418166017, 429499879,
                     437140103, 360410491, 405674907, 399240255, 428510022, 107690666, 436591358, 354087144, 366892993,
                     397547597, 412780655, 421624880, 454027980, 459447099, 459922761, 460014479, 337896240, 453955972,
                     343144461, 399172582, 432855364, 460944916, 462657540, 9118888, 379940541, 382874349, 395713393,
                     418118634, 457397975, 439335164, 439877547, 453955837, 453991943, 453992255, 454010958, 454027869,
                     454035389, 456866249, 457729015, 458018470, 458340825, 458343695, 458575282, 459322916, 459376897,
                     459446977, 459447172, 459447332, 459681903, 459682360, 459685489, 459828314, 460244725, 439659428,
                     94822559, 136433816, 217135980, 228480567, 352353474, 384540834, 389083148, 390789323, 395192773,
                     395193909, 395193980, 395194147, 399296280, 402192937, 414661018, 418118578, 422168729, 422169222,
                     423891740, 424508573, 426636419, 438553615, 422866885, 287413576, 389265538, 415561595, 442410858,
                     442410940, 442410997, 442411175, 448989676, 304910810, 395040317, 399781604, 433596841, 433602713,
                     435692537, 342194960, 423714123, 433609826, 438997483, 369028319, 449450571, 286089126, 394454098,
                     440055899, 443074103, 445289370, 449256882, 449257134, 449257295, 293442386, 316995393, 318354350,
                     321673774, 322919490, 339724085, 356280245, 369027918, 369028135, 454833526, 456618601, 460753660,
                     460754182, 451904668, 451955970, 451956830, 451957405, 452289018, 452289239, 453838804, 454076575,
                     435779264, 435779791, 435780376, 435780553, 438569068, 420621393, 424673226, 428031379, 428477834,
                     429167082, 429196418, 429330085, 430442897, 430443400, 431614731, 431740613, 431741457, 432219751,
                     436813729, 453342653, 296359429, 410120304, 437125058, 445796034, 448238076, 449918247, 449924828,
                     452468131, 457716204, 458405424, 376107803, 378573084, 399090972, 408997193, 411212396, 423141392,
                     425764034, 433391535, 435286815, 450636439, 450996969, 451718071, 454453927, 459799330, 280803901,
                     308821293, 424838311, 432272335, 433563016, 450025721, 450025766, 450026200, 450026254, 452489442,
                     452492668, 455033878, 458998289, 458998334, 458998364, 458998541, 458999166, 458999338, 228534923,
                     305500932, 312010446, 344556471, 348059906, 357406997, 368978980, 419689579, 426933291, 427097181,
                     435490198, 436754970, 437902190, 329734357, 329734438, 329830701, 329831056, 343674480, 360220093,
                     360220895, 365818172, 387914249, 388402453, 393591009, 399621217, 404903421, 404904143, 417971607,
                     423188770, 391255659, 396539567, 396553807, 402246244, 423164761, 425295895, 434359451, 442414544,
                     450894792, 455947758, 52760631, 390026639, 415866604, 397557650, 402461610, 404561768, 408319408,
                     408319487, 439960573, 441615030, 441615054, 454731610, 56751010, 291589197, 302003465, 349149225,
                     351130241, 355606467, 378312676, 399614817, 415703272, 417735507, 426721419, 435197618, 666888,
                     458972696, 458973026, 458973177, 458973263, 458973408, 458973735, 411030750, 424242045, 425412968,
                     425413211, 432988681, 356288789, 357818682, 359027148, 360877919, 363508394, 366162224, 366891957,
                     368503727, 370343440, 378414283, 383921683, 390914942, 397557180, 452407137, 452070061, 451025687,
                     450803412, 450321627, 450218756, 449924702, 449842858, 444759562, 441394196, 413293646, 410441132,
                     395634909, 359784680, 347636905, 300307293, 288197690, 462564643, 457679547, 458268493, 458971896,
                     458972385, 458972580, 458972652, 457335965, 457241905, 457179033, 457010568, 456468935, 455962187,
                     455961791, 455829447, 455635128, 454922102, 454922064, 453839350, 453839074, 453716069, 453410874,
                     452854622, 452489192, 458161841, 458146558, 458054545, 457829914, 457677552, 457585592, 457440059,
                     457336987, 330250336, 353266849, 458972336, 459274800, 459163494, 458911622, 451170971, 449210331,
                     451252819, 413282335, 328413473, 452176205, 127636112, 363933004, 415504957, 252064988, 281138313,
                     127306859, 461684513, 450255031, 449921282, 434531133, 311500340, 414039048, 397011493, 380346487,
                     460635012, 455635271, 455635427, 457318665, 455712347, 357183710, 462561131, 452983392, 423188002,
                     458633171, 49326632, 460892820, 461659904, 411862677, 139226780, 402414249, 448532994, 342595136,
                     449013065, 443418061, 365817145, 458527954, 375708498, 460726804, 413229591, 457272816, 450387907,
                     427346297, 455715605, 216869959, 456497107, 453839608, 455635839, 460645206, 364383339, 365418352,
                     176954444, 443715858, 306580173, 455704812, 370359426, 433589089, 448960434, 454318309, 456610449,
                     453014912, 449963415, 312069018, 453839472, 17073481, 1907608, 301040144, 319174189, 451114723,
                     379056418, 458972382, 455742049, 396612394, 302579357, 457358112, 363584828, 404642667, 305561959,
                     404483621, 409805414, 438641633, 452983434, 9744217, 451903519, 324452859, 73138727, 259582051,
                     286858180, 450425623, 431939086, 240577306, 450795302, 460754824, 440992527, 450366032, 357841666,
                     319791474, 366337033, 449801171, 454922010, 389625993, 450793877, 453839229, 326918172, 82240385,
                     413508315, 450809845, 455615272, 272014948, 450281762, 455830041, 455828162, 456940419, 121738431,
                     456940234, 455829154, 381328303, 443834398, 251962543, 460635272, 462564698, 462563741, 456940063,
                     458885728, 462764815, 457795369, 393623066, 371868729, 16375719, 252221209, 403711124, 455706678,
                     458973137, 458972419, 458973217, 458270014, 457679676, 458973560, 458972951, 458973092, 458972009,
                     382238709, 457675655, 457679174, 456770643, 457679394, 203464860, 458157345, 420467226, 455829752,
                     455830413, 455715334, 456003514, 456413450, 456754937, 458633868, 458634107, 457950036, 457463178,
                     461603886, 457997667, 457997691, 458634246, 458617655, 458617788, 458617575, 458617244, 458891691,
                     458783130, 459863210, 459770872, 458971833, 462488676, 462564380, 458971876, 458971906, 458299218,
                     457241396, 459506800, 462621655, 461823379, 459499240, 458687097, 457721705, 458777423, 461784131,
                     458777291, 459264878, 458146618, 458303576, 458145526, 458633512, 458633598, 458633415, 458633667,
                     458634046, 458634179, 458633782, 458633714, 458634434, 458972113, 459861299, 459860975, 458972076,
                     457648286, 457010400, 457009491, 458972044, 457009252, 457648447, 457010271, 457009134, 457009545,
                     457009422, 457008904, 457009616, 457010147, 457010791, 457009724, 457648317, 457648502, 457648256,
                     458630294, 461658178, 457010329, 457008779, 457009196, 459861056, 461658277, 459861094, 459500568,
                     462561781, 459861133, 457648339, 458972019, 462561676, 461658448, 309981682, 451026328, 460726640,
                     462344574, 457008985, 458143867, 458180576, 461658346, 461658234, 453715677, 422571291, 452854740,
                     333638043, 457335823, 457335660, 379767728, 453715428, 456482837, 296247186, 448914002, 447449594,
                     452854808, 449891607, 419351875, 375856534, 82337487, 298830855, 207201683, 452854904, 453715840,
                     455634770, 455634558, 459759858, 459738011, 457318641, 456006310, 456341218, 455634407, 459738398,
                     455634906, 455635615, 457997602, 457567279, 276323635, 458971941, 451927021, 387706629, 440962338,
                     336625755, 456468971, 336625375, 248402009, 453715277, 458632711, 458630582, 458795540, 458632888,
                     458152259, 460636131, 460518857, 325240457, 461602657, 461603034, 461602703, 458146674, 458196283,
                     459738235, 459738882, 461601273, 462564120, 449221848, 321772161, 459454052, 461939874, 458303793,
                     458309620, 458309167, 458309267, 459630798, 459631244, 458309793, 458610822, 458309963, 458310124,
                     458743533, 458745198, 458303591, 460372413, 461650598, 462642471, 460634758, 462561834, 457335507,
                     457336070, 457335765, 462224723, 458322038, 459761452, 459525704, 459764557, 460860195, 431527913,
                     460851592, 457335891, 462544336, 342034910, 458159582, 461947903, 365430992, 462727739, 457455382,
                     462564804, 462713063, 460874475, 460981536, 460376462, 462602818, 459393219, 460376244, 462632987,
                     459393771, ]

        file_name = 'streamer.xlsx'

        with open(os.path.join(settings.OUTPUT_DIR, file_name), 'w+b') as csv_file:
            workbook = xlsxwriter.Workbook(csv_file)
            worksheet = workbook.add_worksheet()

            worksheet.write_row(0, 0, ['Streamer Id', 'A30', 'A30 PC', 'A30 Web', 'A30 iOS', 'A30 Android',
                                       'V30', 'V30 PC', 'V30 Web', 'V30 iOS', 'V30 Android'])

            for k, streamer_uin in enumerate(streamers):
                print '\b.',
                a30 = self.report_active_users.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                           streamer_uin=streamer_uin)
                a30 = a30[0]['active_users'] if len(a30) > 0 else 0

                a30_pc = self.report_active_users.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                              streamer_uin=streamer_uin, platform=Platform.PC)
                a30_pc = a30_pc[0]['active_users'] if len(a30_pc) > 0 else 0

                a30_web = self.report_active_users.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                               streamer_uin=streamer_uin, platform=Platform.WEB)
                a30_web = a30_web[0]['active_users'] if len(a30_web) > 0 else 0

                a30_ios = self.report_active_users.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                               streamer_uin=streamer_uin, platform=Platform.IOS)
                a30_ios = a30_ios[0]['active_users'] if len(a30_ios) > 0 else 0

                a30_android = self.report_active_users.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                                   streamer_uin=streamer_uin, platform=Platform.ANDROID)
                a30_android = a30_android[0]['active_users'] if len(a30_android) > 0 else 0

                # View Length

                v30 = self.report_total_view_length.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                                streamer_uin=streamer_uin)
                v30 = v30[0]['total_view_length'] if len(v30) > 0 else 0

                v30_pc = self.report_total_view_length.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                                   streamer_uin=streamer_uin, platform=Platform.PC)
                v30_pc = v30_pc[0]['total_view_length'] if len(v30_pc) > 0 else 0

                v30_web = self.report_total_view_length.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                                    streamer_uin=streamer_uin, platform=Platform.WEB)
                v30_web = v30_web[0]['total_view_length'] if len(v30_web) > 0 else 0

                v30_ios = self.report_total_view_length.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                                    streamer_uin=streamer_uin, platform=Platform.IOS)
                v30_ios = v30_ios[0]['total_view_length'] if len(v30_ios) > 0 else 0

                v30_android = self.report_total_view_length.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                                        streamer_uin=streamer_uin,
                                                                        platform=Platform.ANDROID)
                v30_android = v30_android[0]['total_view_length'] if len(v30_android) > 0 else 0

                worksheet.write(k + 1, 0, streamer_uin)
                worksheet.write(k + 1, 1, a30)
                worksheet.write(k + 1, 2, a30_pc)
                worksheet.write(k + 1, 3, a30_web)
                worksheet.write(k + 1, 4, a30_ios)
                worksheet.write(k + 1, 5, a30_android)
                worksheet.write(k + 1, 6, v30)
                worksheet.write(k + 1, 7, v30_pc)
                worksheet.write(k + 1, 8, v30_web)
                worksheet.write(k + 1, 9, v30_ios)
                worksheet.write(k + 1, 10, v30_android)

            workbook.close()

        # ROOM
        print ' '

        rooms = [1102494328, 1102487169, 1102484810, 1102484590, 1102484576, 1102480225, 1102475157, 1102462888,
                 1102457127, 1102447711, 1102438325, 1102438821, 1102431779, 1102427303, 1102429307, 1102419680,
                 1102416935, 1102415730, 1102390684, 1102435815, 1102358224, 1102435417, 1102421787, 1102307500,
                 1102438121, 1102299116, 1102206301, 1102159837, 1102118404, 1102398181, 1102409212, 1102429213,
                 1102397683, 1102248101, 1101425146, 1101424497, 1102405034, 1102421773, 1102396326, 1102415483,
                 1102420995, 1101019358, 1100967459, 1102066337, 1101712775, 1102434945, 1100660697, 1102279929,
                 1100667843, 1102397722, 1100726010, 1101434495, 1100664375, 1100819552, 1100980910, 1102482042,
                 1102318302, 1102496896, 1102495596, 1101611843, 1102103172, 1102505600, 1102498601, 1102513059,
                 1102526049, 1102532185, 1102429727, 1102539582]

        file_name = 'room.xlsx'
        with open(os.path.join(settings.OUTPUT_DIR, file_name), 'w+b') as csv_file:
            workbook = xlsxwriter.Workbook(csv_file)
            worksheet = workbook.add_worksheet()

            worksheet.write_row(0, 0, ['Room Id', 'A30', 'A30 PC', 'A30 Web', 'A30 iOS', 'A30 Android',
                                       'V30', 'V30 PC', 'V30 Web', 'V30 iOS', 'V30 Android', 'Comments'])

            for k, room_id in enumerate(rooms):
                print '\b.',
                a30 = self.report_active_users.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                           room_id=room_id)
                a30 = a30[0]['active_users'] if len(a30) > 0 else 0

                a30_pc = self.report_active_users.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                              room_id=room_id, platform=Platform.PC)
                a30_pc = a30_pc[0]['active_users'] if len(a30_pc) > 0 else 0

                a30_web = self.report_active_users.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                               room_id=room_id, platform=Platform.WEB)
                a30_web = a30_web[0]['active_users'] if len(a30_web) > 0 else 0

                a30_ios = self.report_active_users.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                               room_id=room_id, platform=Platform.IOS)
                a30_ios = a30_ios[0]['active_users'] if len(a30_ios) > 0 else 0

                a30_android = self.report_active_users.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                                   room_id=room_id, platform=Platform.ANDROID)
                a30_android = a30_android[0]['active_users'] if len(a30_android) > 0 else 0

                # View Length

                v30 = self.report_total_view_length.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                                room_id=room_id)
                v30 = v30[0]['total_view_length'] if len(v30) > 0 else 0

                v30_pc = self.report_total_view_length.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                                   room_id=room_id, platform=Platform.PC)
                v30_pc = v30_pc[0]['total_view_length'] if len(v30_pc) > 0 else 0

                v30_web = self.report_total_view_length.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                                    room_id=room_id, platform=Platform.WEB)
                v30_web = v30_web[0]['total_view_length'] if len(v30_web) > 0 else 0

                v30_ios = self.report_total_view_length.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                                    room_id=room_id, platform=Platform.IOS)
                v30_ios = v30_ios[0]['total_view_length'] if len(v30_ios) > 0 else 0

                v30_android = self.report_total_view_length.get_reports(ReportType.REPORT_30_DAY, dt_from, dt_to,
                                                                        room_id=room_id, platform=Platform.ANDROID)
                v30_android = v30_android[0]['total_view_length'] if len(v30_android) > 0 else 0

                total_comments = self.chat_log.get_total_comments_for_room(dt_from, dt_from + relativedelta(months=1),
                                                                           room_id)

                worksheet.write(k + 1, 0, room_id)
                worksheet.write(k + 1, 1, a30)
                worksheet.write(k + 1, 2, a30_pc)
                worksheet.write(k + 1, 3, a30_web)
                worksheet.write(k + 1, 4, a30_ios)
                worksheet.write(k + 1, 5, a30_android)
                worksheet.write(k + 1, 6, v30)
                worksheet.write(k + 1, 7, v30_pc)
                worksheet.write(k + 1, 8, v30_web)
                worksheet.write(k + 1, 9, v30_ios)
                worksheet.write(k + 1, 10, v30_android)
                worksheet.write(k + 1, 11, total_comments)

            workbook.close()

        print ''

    def handle(self, *args, **options):
        dt_from, dt_to = self.read_arguments(options=options)
        self.monthly_report(dt_from, dt_to)
