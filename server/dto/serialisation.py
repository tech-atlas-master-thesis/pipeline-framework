from bson import ObjectId


def custom_json_encoder(obj):
    if isinstance(obj, ObjectId):
        return {"$oid": str(obj)}
    raise TypeError(f"{repr(obj)} of type {type(obj)} is not JSON serializable")
