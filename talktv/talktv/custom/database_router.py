DB_READ_LISTS = (
    ('MonthlyReportByCaster', 'db_report_116'),
    ('anchorgiftlog', 'db_anchor'),
    ('giftinfo', 'db_anchor'),
    ('roomshowtrace', 'db_anchor'),
    ('room_extend', 'db_talktv'),
    ('streamtime_new', 'db_talktv'),
    ('streamtime', 'db_talktv'),
    ('tbl_vcaster_info', 'db_qt_entertainment'),
    ('ztalk_billing_log', 'ztalk'),
    ('ztalk_trans_log', 'ztalk_log'),
)

DB_WRITE_LISTS = (
    ('MonthlyReportByCaster', 'db_report_116'),
)


class DatabaseRouter(object):
    def db_for_read(self, model, **hints):
        for db in DB_READ_LISTS:
            if db[0] in model._meta.db_table:
                return db[1]
        return 'default'

    def db_for_write(self, model, **hints):
        for db in DB_WRITE_LISTS:
            if db[0] in model._meta.db_table:
                return db[1]
        return 'default'
