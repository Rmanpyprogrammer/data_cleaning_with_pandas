
#requirement
import pandas as pd
vosol = pd.read_json('vosol.json')
sabt=pd.read_json("sabt.json")
et=pd.read_json("et.json")
import json
import numpy
#اضافه کردن سرپرست و سوپروایزر به فاکتور ها
et=et.rename(columns={"operator_name":"creator"})
vosol=vosol.merge(et.loc[0:,["creator","group"]],on="creator")
vosol=vosol.merge(et.loc[0:,["creator","leaders"]],on="creator")
sabt=sabt.merge(et.loc[0:,["creator","leaders"]],on="creator")
sabt=sabt.merge(et.loc[0:,["creator","group"]],on="creator")
#مبلغ وصولی
vosoli_hamkadeh=vosol.groupby(["group"])["price"].sum()/10000000
vosoli_hamkadeh=vosoli_hamkadeh.to_frame().reset_index()

vosoli_hamkadeh=vosoli_hamkadeh.rename(columns={"price":"payed"})
#مبلغ وصولی کارتابلی
vosoli_kartabli_hamkadeh=vosol[vosol["factor_type"]=="کارتابلی"]
vosoli_kartabli_hamkadeh=vosoli_kartabli_hamkadeh.groupby(["group"])["price"].sum()/10000000
vosoli_kartabli_hamkadeh=vosoli_kartabli_hamkadeh.to_frame().reset_index()

vosoli_kartabli_hamkadeh=vosoli_kartabli_hamkadeh.rename(columns={"price":"kartabli_payed"})

#مبلغ ثبتی
sabt_hamkadeh=sabt.groupby(["group"])["price"].sum()/10000000
sabt_hamkadeh=sabt_hamkadeh.to_frame().reset_index()

sabt_hamkadeh=sabt_hamkadeh.rename(columns={"price":"paied_of_registration"})
#تعداد ثبت

tedad_sabt_hamkadeh=sabt.groupby(["group"])["price"].count()
tedad_sabt_hamkadeh=tedad_sabt_hamkadeh.to_frame().reset_index()

tedad_sabt_hamkadeh=tedad_sabt_hamkadeh.rename(columns={"price":"number_of_registration"})

#تعداد ثبت کارتابلی

tedad_sabt_kartabli_hamkadeh=sabt[sabt["factor_type"]=="کارتابلی"]
tedad_sabt_kartabli_hamkadeh=tedad_sabt_kartabli_hamkadeh.groupby(["group"])["price"].count()
tedad_sabt_kartabli_hamkadeh=tedad_sabt_kartabli_hamkadeh.to_frame().reset_index()

tedad_sabt_kartabli_hamkadeh=tedad_sabt_kartabli_hamkadeh.rename(columns={"price":"number_of_kartabli_registration"})
#درصد وصولی
sabt_hamkadeh["paid_percent"]=(sabt_hamkadeh.merge(vosoli_hamkadeh,on="group",how="left").fillna(0)["payed"] \
/sabt_hamkadeh.merge(vosoli_hamkadeh,on="group",how="left").fillna(0)["paied_of_registration"])
#مدت مکالمه
modat_mokalemehamkadeh=et.groupby(["group"])["call_duration"].sum()/3600
modat_mokalemehamkadeh=modat_mokalemehamkadeh.to_frame().reset_index()
#تعداد شماره 
tedad_shomare_hamkadeh=et.groupby(["group"])["entries_count"].sum()
tedad_shomare_hamkadeh=tedad_shomare_hamkadeh.to_frame().reset_index()
#تعداد افراد فعال
et=et.fillna(0)
et1=et[et["entries_count"]>0]
number_of_active_saler=et1.groupby(["group"])["creator"].count()
number_of_active_saler=number_of_active_saler.to_frame().reset_index()
#میانگین مبلغ وصولی
number_of_active_saler["av_paied"]=(number_of_active_saler.merge(vosoli_hamkadeh,on="group",how="left").fillna(0)["payed"] \
/number_of_active_saler.merge(vosoli_hamkadeh,on="group",how="left").fillna(0)["creator"])*1000000
#پاسخداد 
pasokhdad_hamkadeh=et.groupby(["group"])["answered_count"].sum()
pasokhdad_hamkadeh=pasokhdad_hamkadeh.to_frame().reset_index()
#ضریب وصولی
pasokhdad_hamkadeh["paid_coe"]=(pasokhdad_hamkadeh.merge(vosoli_kartabli_hamkadeh,on="group",how="left").fillna(0)["kartabli_payed"] \
/pasokhdad_hamkadeh.merge(vosoli_kartabli_hamkadeh,on="group",how="left").fillna(0)["answered_count"])*1000000
#درصد پاسخداد
pasokhdad_hamkadeh["answered_percent"]=(pasokhdad_hamkadeh.merge(tedad_shomare_hamkadeh,on="group",how="left").fillna(0)["answered_count"] \
/pasokhdad_hamkadeh.merge(tedad_shomare_hamkadeh,on="group",how="left").fillna(0)["entries_count"])
#درصد خرید
pasokhdad_hamkadeh["bought_percent"]=(pasokhdad_hamkadeh.merge(tedad_sabt_kartabli_hamkadeh,on="group",how="left").fillna(0)["number_of_kartabli_registration"] \
/pasokhdad_hamkadeh.merge(tedad_sabt_kartabli_hamkadeh,on="group",how="left").fillna(0)["answered_count"])

#ساخت فایل jsonنهایی
data = {
    "vosoli_hamkadeh": vosoli_hamkadeh,
    "vosoli_kartabli_hamkadeh": vosoli_kartabli_hamkadeh,
    "sabt_hamkadeh": sabt_hamkadeh,
    "tedad_sabt_hamkadeh": tedad_sabt_hamkadeh,
    "tedad_sabt_kartabli_hamkadeh": tedad_sabt_kartabli_hamkadeh,
    "modat_mokalemehamkadeh": modat_mokalemehamkadeh,
    "tedad_shomare_hamkadeh": tedad_shomare_hamkadeh,
    "pasokhdad_hamkadeh": pasokhdad_hamkadeh,
    "number_of_active_saler":number_of_active_saler
}





# تبدیل داده‌ها به دیتافریم
df = pd.concat(data.values(), axis=1, keys=data.keys())
json_data = df.to_json(orient="index")



data = json.loads(json_data)
supervisor_json = json.dumps(data, ensure_ascii=False, indent=4)

#save
# with open("output.json", "w", encoding="utf-8") as fp:
#     fp.write(supervisor_json)

print(df)