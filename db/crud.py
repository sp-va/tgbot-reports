import datetime

from db.connection import collection


class GetReport:
    @staticmethod
    def add_timezone(timestamp: str):
        timestamp = datetime.datetime.fromisoformat(timestamp)
        timezone_added = timestamp.replace(tzinfo=datetime.timezone.utc)

        return timezone_added

    @staticmethod
    async def by_months(dt_from: str, dt_upto: str):
        dataset = []
        labels = []
        dt_from = GetReport.add_timezone(dt_from)
        dt_upto = GetReport.add_timezone(dt_upto)

        query = collection.aggregate([
            {
                '$match': {
                    'dt': {
                        '$gte': dt_from,
                        '$lte': dt_upto
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'year': {
                            '$year': '$dt'
                        },
                        'month': {
                            '$month': '$dt'
                        }
                    },
                    'dataset': {
                        '$sum': '$value'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'dataset': 1,
                    'labels': {
                        '$dateFromParts': {
                            'year': '$_id.year',
                            'month': '$_id.month'
                        }
                    }
                }
            }, {
                '$sort': {
                    'labels': 1
                }
            }
        ])
        result = await query.to_list(length=100)

        for i in result:
            dataset.append(i["dataset"])
            time_str = i["labels"]
            time_str = time_str.isoformat()
            labels.append(time_str)
        return {"dataset": dataset, "labels": labels}

    @staticmethod
    async def by_days(dt_from: str, dt_upto: str):
        dataset = []
        labels = []
        dt_from = GetReport.add_timezone(dt_from)
        dt_upto = GetReport.add_timezone(dt_upto)

        query = collection.aggregate([
            {
                '$match': {
                    'dt': {
                        '$gte': dt_from,
                        '$lte': dt_upto
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'year': {
                            '$year': '$dt'
                        },
                        'month': {
                            '$month': '$dt'
                        },
                        'day': {
                            '$dayOfMonth': '$dt'
                        }
                    },
                    'dataset': {
                        '$sum': '$value'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'dataset': 1,
                    'labels': {
                        '$dateFromParts': {
                            'year': '$_id.year',
                            'month': '$_id.month',
                            'day': '$_id.day'
                        }
                    }
                }
            }, {
                '$sort': {
                    'labels': 1
                }
            }
        ])
        result = await query.to_list(length=100)

        for i in result:
            dataset.append(i["dataset"])
            time_str = i["labels"]
            time_str = time_str.isoformat()
            labels.append(time_str)
        return {"dataset": dataset, "labels": labels}

    @staticmethod
    async def by_hours(dt_from: str, dt_upto: str):
        dataset = []
        labels = []
        dt_from = GetReport.add_timezone(dt_from)
        dt_upto = GetReport.add_timezone(dt_upto)

        query = collection.aggregate([
            {
                '$match': {
                    'dt': {
                        '$gte': dt_from,
                        '$lte': dt_upto
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'year': {
                            '$year': '$dt'
                        },
                        'month': {
                            '$month': '$dt'
                        },
                        'day': {
                            '$dayOfMonth': '$dt'
                        },
                        'hour': {
                            '$hour': '$dt'
                        }
                    },
                    'dataset': {
                        '$sum': '$value'
                    }
                }
            }, {
                '$project': {
                    '_id': 0,
                    'dataset': 1,
                    'labels': {
                        '$dateFromParts': {
                            'year': '$_id.year',
                            'month': '$_id.month',
                            'day': '$_id.day',
                            'hour': '$_id.hour'
                        }
                    }
                }
            }, {
                '$sort': {
                    'labels': 1
                }
            }
        ])
        result = await query.to_list(length=100)

        for i in result:
            dataset.append(i["dataset"])
            time_str = i["labels"]
            time_str = time_str.isoformat()
            labels.append(time_str)
        return {"dataset": dataset, "labels": labels}


GROUP_TYPES = {
    "hour": GetReport.by_hours,
    "day": GetReport.by_days,
    "month": GetReport.by_months,
}



