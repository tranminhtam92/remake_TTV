from django.db import models

dict_AnchorGiftLog_model = {}


def get_AnnchorGiftLog_model(anchor_uin):
    k = str(anchor_uin % 100)
    if k not in dict_AnchorGiftLog_model:
        class Meta:
            db_table = 'anchorgiftlog{}'.format(k)

        attrs = {
            'Meta': Meta,
            'user_uin': models.PositiveIntegerField(),
            'anchor_uin': models.PositiveIntegerField(),
            'room_id': models.PositiveIntegerField(),
            'gift_id': models.PositiveIntegerField(),
            'gift_num': models.PositiveIntegerField(),
            'give_time': models.DateTimeField(),
            'objects': models.Manager(),
            '__module__': 'log_event.models',
        }
        dict_AnchorGiftLog_model[k] = type("AnnchorGiftLog{}".format(k), (models.Model,), attrs)
    return dict_AnchorGiftLog_model[k]
