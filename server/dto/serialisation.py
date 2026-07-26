import datetime

import pandas as pd
from bson import ObjectId


def custom_json_encoder(obj):
    if isinstance(obj, ObjectId):
        return {"$oid": str(obj)}
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    raise TypeError(f"{repr(obj)} of type {type(obj)} is not JSON serializable")
