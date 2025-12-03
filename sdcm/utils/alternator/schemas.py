from sdcm.utils.alternator import consts
from sdcm.utils.alternator import enums

HASH_SCHEMA = dict(
    KeySchema=[
        {"AttributeName": f"{consts.HASH_KEY_NAME}", "KeyType": "HASH"},
    ],
    AttributeDefinitions=[
        {"AttributeName": f"{consts.HASH_KEY_NAME}", "AttributeType": "S"},
    ],
)

HASH_AND_STR_RANGE_SCHEMA = dict(
    KeySchema=[
        {"AttributeName": f"{consts.HASH_KEY_NAME}", "KeyType": "HASH"},
        {"AttributeName": f"{consts.RANGE_KEY_NAME}", "KeyType": "RANGE"},
    ],
    AttributeDefinitions=[
        {"AttributeName": f"{consts.HASH_KEY_NAME}", "AttributeType": "S"},
        {"AttributeName": f"{consts.RANGE_KEY_NAME}", "AttributeType": "S"},
    ],
)

ALTERNATOR_SCHEMAS = {
    enums.YCSBSchemaTypes.HASH_SCHEMA.value: HASH_SCHEMA,
    enums.YCSBSchemaTypes.HASH_AND_RANGE.value: HASH_AND_STR_RANGE_SCHEMA,
}
