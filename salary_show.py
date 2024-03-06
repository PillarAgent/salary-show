import datetime
import bson
import pandas as pd


def open_file_bson(file_bson) -> list:
    return [{str(k): v for k, v in i.items()} for i in bson.decode_all(open(file_bson, 'rb').read())]


def group_simbol(x: str) -> str:
    if x.lower() == 'month':
        return 'ME'
    elif x.lower() == 'year':
        return 'YE'
    else:
        return x.upper()[0]


test_input = {
    "dt_from": "2022-09-01T00:00:00",
    "dt_upto": "2022-12-31T23:59:00",
    "group_type": "month"
}

dt_start = datetime.datetime.fromisoformat(test_input['dt_from'])
dt_finish = datetime.datetime.fromisoformat(test_input['dt_upto'])
group_type = group_simbol(test_input['group_type'])

data = open_file_bson('sample_collection.bson')
columns = ['_id', 'value', 'dt']
data_set = pd.DataFrame(data, columns=columns)
data_set.index = pd.to_datetime(data_set.dt)
data_set.drop('_id', axis=1, inplace=True)

period = data_set[(data_set.index >= dt_start) & (data_set.index <= dt_finish)]

res = period.value.resample(group_type).sum()

dataset = res.tolist()
labels = [str(datetime.datetime.isoformat(label)) for label in res.index]
result = {"dataset": dataset, "labels": labels}
print(result)
