class Category:
    NORMAL_ROOM = 0
    IDOL_ROOM = 1
    PC_GAME = 2
    MOBILE_GAME = 3
    LIVE_MOBILE = 4
    OTHERS = 9


CATEGORY_IDS = (
    (None, "All categories"),
    (Category.NORMAL_ROOM, 'Normal'),
    (Category.IDOL_ROOM, 'Idol'),
    (Category.PC_GAME, 'Game_PC'),
    (Category.MOBILE_GAME, 'Game_Mobile'),
    (Category.LIVE_MOBILE, 'Live_Mobile'),
    (Category.OTHERS, 'Other'),
)


class SubCategory:
    IDOL_SING = 101
    IDOL_DANCE = 102
    IDOL_TALK = 103
    IDOL_OTHERS = 199

    PC_LOL = 201
    PC_FPS = 202
    PC_AOE = 203
    PC_OTHERS = 299

    MOBILE_LQ = 301
    MOBILE_CF = 302
    MOBILE_OTHERS = 399

    LIVE_MOBILE_SING = 401
    LIVE_MOBILE_DANCE = 402
    LIVE_MOBILE_TALK = 403
    LIVE_MOBILE_OTHERS = 499

    OTHERS = None


SUB_CATEGORY_IDS = (
    (None, "All sub-categories"),
    # Idol
    (SubCategory.IDOL_SING, 'Idol_Sing'),
    (SubCategory.IDOL_DANCE, 'Idol_Dance'),
    (SubCategory.IDOL_TALK, 'Idol_Talk'),
    (SubCategory.IDOL_OTHERS, 'Idol_Other'),
    # Game PC
    (SubCategory.PC_LOL, 'Game_PC_LOL'),
    (SubCategory.PC_FPS, 'Game_PC_FPS'),
    (SubCategory.PC_AOE, 'Game_PC_AOE'),
    (SubCategory.PC_OTHERS, 'Game_PC_Other'),
    # Game Mobile
    (SubCategory.MOBILE_LQ, 'Game_Mobile_LQ'),
    (SubCategory.MOBILE_CF, 'Game_Mobile_CF'),
    (SubCategory.MOBILE_OTHERS, 'Game_Mobile_Other'),
    # Live Mobile
    (SubCategory.LIVE_MOBILE_SING, 'Live_Mobile_Sing'),
    (SubCategory.LIVE_MOBILE_DANCE, 'Live_Mobile_Dance'),
    (SubCategory.LIVE_MOBILE_TALK, 'Live_Mobile_Talk'),
    (SubCategory.LIVE_MOBILE_OTHERS, 'Live_Mobile_Other'),
)


class Platform:
    ALL = None
    PC = 0
    WEB = 100
    GOOGLE_ANALYTICS = 101
    IOS = 200
    ANDROID = 201


PLATFORM_IDS = (
    (Platform.ALL, "All platforms"),
    (Platform.PC, 'PC'),
    (Platform.WEB, 'Web'),
    (Platform.GOOGLE_ANALYTICS, 'Web_GA'),
    (Platform.IOS, 'Mobile_iOS'),
    (Platform.ANDROID, 'Mobile_Android'),
)

DANCE_ROOM_IDS = (69, 96)


class PCGameID:
    LOL = 112
    FPS = 164
    AOE = 113
    OTHERS = 0


class MobileGameID:
    LIEN_QUAN = 160
    CROSSFIRE_LEGENDS = 161
    OTHERS = 163


MOBILE_GAME_IDS = (
    MobileGameID.LIEN_QUAN,  # 160: Lien Quan
    MobileGameID.CROSSFIRE_LEGENDS,  # 161: CrossFire Legends
    MobileGameID.OTHERS,  # 163: Game Mobile Others
)


class GamePlatform:
    PC = 0
    Mobile = 1


LOG_LEAVEROOM_KEYS = (
    'file_type', 'game_id', 'uin', 'room_id',
    'sub_room_id_1', 'sub_room_id_2', 'room_layer', 'room_type',
    'source_type', 'view_length', 'login_ip', 'stop_time',
    'is_sub_room', 'platform', 'room_mode'
)

LOG_CHAT_KEYS = (
    'file_type', 'game_id', 'uin', 'nick_name', 'sex',
    'room_id', 'sub_room_id_1', 'sub_room_id_2', 'room_layer', 'room_type',
    'source_type', 'room_name', 'nick_name_in_room', 'user_type',
    'login_ip', 'chat_type', 'event_time', 'chat_content', 'dest_uin',
    'dest_nick_name', 'dest_login_ip', 'dest_nick_name_in_room', 'dest_user_type'
)

LOG_MOBILE_KEYS = (
    'file_type', 'uin', 'login_ip', 'platform', 'start_time', 'stop_time',
    'room_id', 'room_mode', '_file_enter', '_file_quit'
)

LOG_MOBILE_ENTER_KEYS = (
    'file_type', 'uin', 'platform', 'room_id',
    'sub_room_id_1', 'login_ip', 'start_time', 'room_mode'
)

LOG_MOBILE_QUIT_KEYS = (
    'file_type', 'uin', 'platform', 'room_id',
    'sub_room_id_1', 'login_ip', 'stop_time', 'room_mode'
)

CONVERT_GAME_CATEGORY_IDS_TO_GAME_IDS = {
    3: MobileGameID.LIEN_QUAN,
    4: MobileGameID.CROSSFIRE_LEGENDS,
    7: MobileGameID.OTHERS,
    12: PCGameID.LOL,
    14: PCGameID.FPS,
    17: PCGameID.OTHERS,
}

CONVERT_GAME_CATEGORY_IDS = {
    3: SubCategory.MOBILE_LQ,
    4: SubCategory.MOBILE_CF,
    7: SubCategory.MOBILE_OTHERS,
    12: SubCategory.PC_LOL,
    14: SubCategory.PC_FPS,
    17: SubCategory.OTHERS,
}

CONVERT_IDOL_CATEGORY_IDS = {
    18: SubCategory.IDOL_SING,
    19: SubCategory.IDOL_DANCE,
    20: SubCategory.IDOL_TALK,
    21: SubCategory.IDOL_OTHERS,
}
CONVERT_LM_CATEGORY_IDS = {
    8: SubCategory.LIVE_MOBILE_SING,
    9: SubCategory.LIVE_MOBILE_DANCE,
    10: SubCategory.LIVE_MOBILE_TALK,
    11: SubCategory.LIVE_MOBILE_OTHERS,
}

CONVERT_CATEGORY_IDS = dict(
    CONVERT_GAME_CATEGORY_IDS.items() +
    CONVERT_IDOL_CATEGORY_IDS.items() +
    CONVERT_LM_CATEGORY_IDS.items()
)

MAX_STREAM_LENGTH = 864000  # 10 days

LOG_VIEW = 'view'
LOG_CHAT = 'chat'
LOG_MOBILE = 'mobile'
LOG_TYPE_CHOICES = ((LOG_VIEW, 'view'),
                    (LOG_CHAT, 'chat'),
                    (LOG_MOBILE, 'mobile proxy'),)

RESULT_SUCCESS = 'success'
RESULT_WARNING = 'warning'
RESULT_FAILED = 'failed'
RESULT_WAITING = 'waiting'
RESULT_CHOICES = ((RESULT_SUCCESS, 'success'),
                  (RESULT_WARNING, 'warning'),
                  (RESULT_FAILED, 'failed'),
                  (RESULT_WAITING, 'waiting'),)


class ReportType:
    HOURLY_REPORT = 'hourly_report'
    REPORT_1_DAY = '1 day'
    REPORT_7_DAYS = '7 days'
    REPORT_30_DAYS = '30 days'
    STREAMING = 'streaming_report'


REPORT_TYPE_CHOICES = (
    (ReportType.HOURLY_REPORT, 'hourly_report'),
    (ReportType.REPORT_1_DAY, '1 day'),
    (ReportType.REPORT_7_DAYS, '7 days'),
    (ReportType.REPORT_30_DAYS, '30 days'),
    (ReportType.STREAMING, 'streaming_report'),
)

RANK_CHOICES = (
    (1, 'S'),
    (2, 'A'),
    (3, 'B'),
    (4, 'C')
)
