import pandas as pd
import json

vosol = pd.read_json('vosol.json')
sabt = pd.read_json('sabt.json')
et = pd.read_json('et.json')

et = et.rename(columns={"operator_name": "creator"})

vosol = vosol.merge(et.loc[:, ["creator", "group"]], on="creator")
vosol = vosol.merge(et.loc[:, ["creator", "leaders"]], on="creator")
sabt = sabt.merge(et.loc[:, ["creator", "leaders"]], on="creator")
sabt = sabt.merge(et.loc[:, ["creator", "group"]], on="creator")

vosoli_hamkadeh = vosol.groupby("group").agg(payed=("price", "sum"))
vosoli_hamkadeh["payed"] /= 10000000

vosoli_kartabli_hamkadeh = vosol[vosol["factor_type"] == "کارتابلی"].groupby("group").agg(kartabli_payed=("price", "sum"))
vosoli_kartabli_hamkadeh["kartabli_payed"] /= 10000000

sabt_hamkadeh = sabt.groupby("group").agg(paied_of_registration=("price", "sum"))
sabt_hamkadeh["paied_of_registration"] /= 10000000

tedad_sabt_hamkadeh = sabt.groupby("group").size().reset_index(name="number_of_registration")

tedad_sabt_kartabli_hamkadeh = sabt[sabt["factor_type"] == "کارتابلی"].groupby("group").size().reset_index(name="number_of_kartabli_registration")

modat_mokalemehamkadeh = et.groupby("group").agg(modat_mokalemehamkadeh=("call_duration", "sum"))
modat_mokalemehamkadeh["modat_mokalemehamkadeh"] /= 3600

tedad_shomare_hamkadeh = et.groupby("group").agg(tedad_shomare_hamkadeh=("entries_count", "sum"))

pasokhdad_hamkadeh = et.groupby("group").agg(pasokhdad_hamkadeh=("answered_count", "sum"))

# ترتیب داده‌ها
data = {
    "vosoli_hamkadeh": vosoli_hamkadeh,
    "vosoli_kartabli_hamkadeh": vosoli_kartabli_hamkadeh,
    "sabt_hamkadeh": sabt_hamkadeh,
    "tedad_sabt_hamkadeh": tedad_sabt_hamkadeh,
    "tedad_sabt_kartabli_hamkadeh": tedad_sabt_kartabli_hamkadeh,
    "modat_mokalemehamkadeh": modat_mokalemehamkadeh,
    "tedad_shomare_hamkadeh": tedad_shomare_hamkadeh,
    "pasokhdad_hamkadeh": pasokhdad_hamkadeh
}

# تبدیل داده‌ها به دیتافریم
df = pd.concat(data.values(), axis=1, keys=data.keys())

# تبدیل دیتافریم به فرمت JSON
json_data = df.to_json(orient="index")

# ذخیره JSON در یک فایل

import pandas as pd

vosol = pd.read_json('vosol.json')
sabt = pd.read_json('sabt.json')
et = pd.read_json('et.json')

et = et.rename(columns={"operator_name": "creator"})

vosol = vosol.merge(et.loc[:, ["creator", "group"]], on="creator")
vosol = vosol.merge(et.loc[:, ["creator", "leaders"]], on="creator")
sabt = sabt.merge(et.loc[:, ["creator", "leaders"]], on="creator")
sabt = sabt.merge(et.loc[:, ["creator", "group"]], on="creator")

vosoli_hamkadeh = vosol.groupby("group").agg(payed=("price", "sum"))
vosoli_hamkadeh["payed"] /= 10000000

vosoli_kartabli_hamkadeh = vosol[vosol["factor_type"] == "کارتابلی"].groupby("group").agg(kartabli_payed=("price", "sum"))
vosoli_kartabli_hamkadeh["kartabli_payed"] /= 10000000

sabt_hamkadeh = sabt.groupby("group").agg(paied_of_registration=("price", "sum"))
sabt_hamkadeh["paied_of_registration"] /= 10000000

tedad_sabt_hamkadeh = sabt.groupby("group").size().reset_index(name="number_of_registration")

tedad_sabt_kartabli_hamkadeh = sabt[sabt["factor_type"] == "کارتابلی"].groupby("group").size().reset_index(name="number_of_kartabli_registration")

modat_mokalemehamkadeh = et.groupby("group").agg(modat_mokalemehamkadeh=("call_duration", "sum"))
modat_mokalemehamkadeh["modat_mokalemehamkadeh"] /= 3600

tedad_shomare_hamkadeh = et.groupby("group").agg(tedad_shomare_hamkadeh=("entries_count", "sum"))

pasokhdad_hamkadeh = et.groupby("group").agg(pasokhdad_hamkadeh=("answered_count", "sum"))

# ترتیب داده‌ها
data = {
    "vosoli_hamkadeh": vosoli_hamkadeh,
    "vosoli_kartabli_hamkadeh": vosoli_kartabli_hamkadeh,
    "sabt_hamkadeh": sabt_hamkadeh,
    "tedad_sabt_hamkadeh": tedad_sabt_hamkadeh,
    "tedad_sabt_kartabli_hamkadeh": tedad_sabt_kartabli_hamkadeh,
    "modat_mokalemehamkadeh": modat_mokalemehamkadeh,
    "tedad_shomare_hamkadeh": tedad_shomare_hamkadeh,
    "pasokhdad_hamkadeh": pasokhdad_hamkadeh
}

# تبدیل داده‌ها به دیتافریم
df = pd.concat(data.values(), axis=1, keys=data.keys())
json_data = df.to_json(orient="index")
data = json.loads(json_data)
formatted_json = json.dumps(data, ensure_ascii=False, indent=4)

# تبدیل دیتافریم به فرمت JSON





with open('final_data.json', 'w') as file:
    file.write(formatted_json)