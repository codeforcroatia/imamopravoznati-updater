# -*- coding: utf-8 -*-
"""
Created on Fri Jul 19 14:28:26 2019

@author: Saifullah
"""

import pandas as pd

base_data = pd.read_csv("https://api.morph.io/SelectSoft/blue_gene/data.csv?key=7hDDGSosf23K7474Bd4P&query=select%20*%20from%20data" ,error_bad_lines=False,sep=',')


base_data["vat_number"] =base_data.vat_number.astype('str')
#base_data['vat_number'] = base_data['vat_number'].astype(str).replace('\.0', '', regex=True)
base_data['vat_number'] = base_data['vat_number'].apply(lambda x: x.zfill(11))
# Getting the data that is already in a server or localhost data


server_data = pd.read_csv("Data/authority-export.csv",error_bad_lines=False,sep=',')

# Seprating the OIB from tags

server_data["vat_number"] = server_data['tag_string'].str.extract('(\d+)')


# remove from processing when tag_string contain defunct 
server_data= server_data.dropna(subset=['tag_string'])

remFlag = server_data.tag_string.str.contains("defunct")
skip = server_data[remFlag]
server_data = server_data[~remFlag]
skip= skip.dropna(subset=['vat_number'])
base_data  = base_data[~base_data.vat_number.isin(skip.vat_number)]



remFlag = server_data.tag_string.str.contains("auto:skip")
skip = server_data[remFlag]
server_data = server_data[~remFlag]
skip= skip.dropna(subset=['vat_number'])
base_data  = base_data[~base_data.vat_number.isin(skip.vat_number)]

remFlag = server_data.tag_string.str.contains("oib:null")
skip = server_data[remFlag]
server_data = server_data[~remFlag]
skip= skip.dropna(subset=['vat_number'])
base_data  = base_data[~base_data.vat_number.isin(skip.vat_number)]

remFlag = server_data.tag_string.str.contains("oib:nan")
skip = server_data[remFlag]
server_data = server_data[~remFlag]
skip= skip.dropna(subset=['vat_number'])
base_data  = base_data[~base_data.vat_number.isin(skip.vat_number)]

remFlag = server_data.tag_string.str.contains("oib-null")
skip = server_data[remFlag]
server_data = server_data[~remFlag]
skip= skip.dropna(subset=['vat_number'])
base_data  = base_data[~base_data.vat_number.isin(skip.vat_number)]


# splitting columns

foo = lambda x: pd.Series([i for i in reversed(x.split(' '))])
rev = server_data['tag_string'].apply(foo)
searchfor = ["rh","samouprava","javnopravno-tijelo","pravna-osoba",
             "drzavna-tijela","drzavna-uprava",
             "jedinice-samouprave","sudovi","agencije",
             "javne-ustanove","trgovacka-drustva","udruge",
             "ostale-pravne-osobe","javna-uprava-politicki",
             "javna-uprava-politicki","obrana-sigurnost",
             "javni-red","pravosudje","javne-financije","vanjski-poslovi","gospodarstvo",
             "promet-komunikacije","obrazovanje","kultura-umjetnost","zaposljavanje",
             "zaposljavanje", "socijalna-zastita","zdravstvo","poljoprivreda",
             "komunalne-usluge","okolis","regionalni-razvoj","turizam","statistika-informatika-dokumentacija",
             "hidrometeorologija","ostalo","javna-ustanova"]
i = 0
for x in rev.columns:
    f = rev[i].str.contains('|'.join(searchfor))
    f = f.fillna(False)
    rev[i] = rev[~f][i]
    f = rev[i].str.contains('(\d+)')
    f = f.fillna(False)
    rev[i] = rev[~f][i]
    i = i + 1
    
server_data['extra_tag'] = rev.apply(lambda x: ' '.join(x[x.notnull()]), axis = 1)



# Updated    Data that is in both file will come in updated variable

updatedFlagServer = server_data['vat_number'].isin(base_data['vat_number']) & (server_data['vat_number'].notnull())
updated = server_data[updatedFlagServer]


# Change status to update

updated['status'] = 'updated'
updated["auto"] = 'auto:updated'


removedflag = server_data['vat_number'].isin(base_data['vat_number'])
removed = server_data[~removedflag]



removed["transfer_mail"] = removed['request_email']
removed["transfer_web"] = removed['home_page']
removed = removed.drop({'request_email'},1)



# Change status to removed

removed['status'] = "removed"
removed["tag_string"] = removed.tag_string.astype(str) + " auto:removed " + "defunct"
# new

newFlag = base_data['vat_number'].isin(server_data['vat_number'])  & (base_data['vat_number'].notnull())
new = base_data[~newFlag]


# Change status to new

new["status"] = "new"
new["auto"] = 'auto:created'

#/******************************************
new['founder'] = new['founder'].replace({"Republika Hrvatska":"rh",
       "Republika Hrvatska osnivač tijela javne vlasti":"rh",
       "Jedinica lokalne ili područne samouprave":"samouprava", 
       "Jedinica lokalne ili područne (regionalne) samouprave je osnivač":"samouprava",
       "Jedinica lokalne ili područne (regionalne) samouprave":"samouprava",
        "Javnopravno tijelo":"javnopravno-tijelo",
        "Javnopravno tijelo ili tijelo s prenesenim javnim ovlastima":"javnopravno-tijelo",
        "Javnopravno tijelo ili tijelo s prenesenim javnim ovlastima je osnivač tijela javne vlasti":"javnopravno-tijelo",
        "Fizička ili privatna pravna osoba":"pravna-osoba", 
        "Fizička ili privatna pravna osoba je osnivač":"pravna-osoba"})



new['legal_status'] = new['legal_status'].replace({"Državna tijela":"drzavna-tijela",
       "Tijela državne uprave":"drzavna-uprava",
       "Jedinice lokalne ili područne samouprave":"jedinice-samouprave", 
       "Jedinica lokalne ili područne (regionalne) samouprave":"jedinice-samouprave",
       "Sudovi i pravosudna tijela":"sudovi",
       "Agencije, zavodi, fondovi, centri":"agencije",
       "Agencije, zavodi, fondovi, centri i druge samostalne pravne osobe s javnim ovlastima":"agencije",
       "Agencije i druge samostalne pravne osobe s javnim ovlastima RH":"agencije",
       "Javne ustanove":"javne-ustanove",
       "Trgovačka društva":"trgovacka-drustva",
       "Udruge":"udruge",
       "Udruge i organizacije civilnog društva":"udruge",
       "Ostale pravne i fizičke osobe":"ostale-pravne-osobe",
       "Ostale pravne i fizičke osobe s prenesenim javnim ovlastima":"ostale-pravne-osobe",
       "Ostale pravne osobe i tijela s javnim ovlastima":"ostale-pravne-osobe",
       "Ustanove":"javne-ustanove"
       })

new['topics'] = new['topics'].replace({"Javna uprava i politički sustav":"javna-uprava-politicki",
       "Javna uprava i politički sustav":"javna-uprava-politicki",
       "Obrana i nacionalna sigurnost":"obrana-sigurnost",
       "Javni red i sigurnost":"javni-red",
       "Pravosuđe":"pravosudje",
       "Javne financije":"javne-financije",
       "Vanjski poslovi":"vanjski-poslovi",
       "Gospodarstvo":"gospodarstvo",
       "Promet i komunikacije":"promet-komunikacije",
       "Odgoj, obrazovanje, znanost i sport": "obrazovanje",
       "Kultura i umjetnost":"kultura-umjetnost", 
       "Zapošljavanje, rad i radni odnosi":"zaposljavanje",
       "Zapošljavanje, rad i radni odnosi":"zaposljavanje", 
       "Socijalna zaštita":"socijalna-zastita",
       "Socijalna zaštita":"socijalna-zastita", 
       "Zdravstvo":"zdravstvo", 
       "Poljoprivreda, šumarstvo i veterinarstvo":"poljoprivreda", 
       "Komunalne usluge i vodno gospodarstvo":"komunalne-usluge",
       "Zaštita okoliša i održivi razvoj":"okolis", 
       "Regionalni razvoj":"regionalni-razvoj",
       "Turizam":"turizam",
       "Statistika i informacijsko-dokumentacijska djelatnost":"statistika-informatika-dokumentacija",
       "Hidrometeorološka djelatnost":"hidrometeorologija",
       "Ostalo - neklasificirane djelatnosti":"ostalo"
       })

base_data = base_data.drop_duplicates(subset=['vat_number'], keep='first')
server_data = server_data.drop_duplicates(subset=['vat_number'], keep='first')



updated['request_email'] = updated['vat_number'].map(base_data.set_index('vat_number')['foi_officer_email'])
updated['request_email'] = updated['request_email'].fillna(updated['vat_number'].map(base_data.set_index('vat_number')['email']))
updated['request_email'] = updated['request_email'].fillna(updated['vat_number'].map(server_data.set_index('vat_number')['request_email']))


updated['home_page'] = updated['vat_number'].map(base_data.set_index('vat_number')['website'])
updated['home_page'] = updated['home_page'].fillna(updated['vat_number'].map(server_data.set_index('vat_number')['home_page']))

allData = pd.concat([updated , new , removed]);
allData = allData.drop_duplicates(subset=['vat_number'], keep='first')

new["oib_tag"] = "oib:" + new.vat_number.astype(str)

mask = allData['status'].str.contains(('new'), na=True)
allData.loc[mask, 'tag_string'] = new[['legal_status','founder','topics','auto','oib_tag']].apply(lambda x: None if x.isnull().all() else ' '.join(x.dropna()), axis=1)


#/*************************************************

allData['founder']  = allData['vat_number'].map(base_data.set_index('vat_number')['founder'])
allData['legal_status'] = allData['vat_number'].map(base_data.set_index('vat_number')['legal_status'])
allData['topics'] = allData['vat_number'].map(base_data.set_index('vat_number')['topics'])


allData["oib_tag"] = "oib:" + allData.vat_number.astype(str)



# replace actual category to tags 


allData['founder'] = allData['founder'].replace({"Republika Hrvatska":"rh",
       "Republika Hrvatska osnivač tijela javne vlasti":"rh",
       "Jedinica lokalne ili područne samouprave":"samouprava", 
       "Jedinica lokalne ili područne (regionalne) samouprave je osnivač":"samouprava",
       "Jedinica lokalne ili područne (regionalne) samouprave":"samouprava",
        "Javnopravno tijelo":"javnopravno-tijelo",
        "Javnopravno tijelo ili tijelo s prenesenim javnim ovlastima":"javnopravno-tijelo",
        "Javnopravno tijelo ili tijelo s prenesenim javnim ovlastima je osnivač tijela javne vlasti":"javnopravno-tijelo",
        "Fizička ili privatna pravna osoba":"pravna-osoba", 
        "Fizička ili privatna pravna osoba je osnivač":"pravna-osoba"})



allData['legal_status'] = allData['legal_status'].replace({"Državna tijela":"drzavna-tijela",
       "Tijela državne uprave":"drzavna-uprava",
       "Jedinice lokalne ili područne samouprave":"jedinice-samouprave", 
       "Jedinica lokalne ili područne (regionalne) samouprave":"jedinice-samouprave",
       "Sudovi i pravosudna tijela":"sudovi",
       "Agencije, zavodi, fondovi, centri":"agencije",
       "Agencije, zavodi, fondovi, centri i druge samostalne pravne osobe s javnim ovlastima":"agencije",
       "Agencije i druge samostalne pravne osobe s javnim ovlastima RH":"agencije",
       "Javne ustanove":"javne-ustanove",
       "Trgovačka društva":"trgovacka-drustva",
       "Udruge":"udruge",
       "Udruge i organizacije civilnog društva":"udruge",
       "Ostale pravne i fizičke osobe":"ostale-pravne-osobe",
       "Ostale pravne i fizičke osobe s prenesenim javnim ovlastima":"ostale-pravne-osobe",
       "Ostale pravne osobe i tijela s javnim ovlastima":"ostale-pravne-osobe",
       "Ustanove":"javne-ustanove"
       })

allData['topics'] = allData['topics'].replace({"Javna uprava i politički sustav":"javna-uprava-politicki",
       "Javna uprava i politički sustav":"javna-uprava-politicki",
       "Obrana i nacionalna sigurnost":"obrana-sigurnost",
       "Javni red i sigurnost":"javni-red",
       "Pravosuđe":"pravosudje",
       "Javne financije":"javne-financije",
       "Vanjski poslovi":"vanjski-poslovi",
       "Gospodarstvo":"gospodarstvo",
       "Promet i komunikacije":"promet-komunikacije",
       "Odgoj, obrazovanje, znanost i sport": "obrazovanje",
       "Kultura i umjetnost":"kultura-umjetnost", 
       "Zapošljavanje, rad i radni odnosi":"zaposljavanje",
       "Zapošljavanje, rad i radni odnosi":"zaposljavanje", 
       "Socijalna zaštita":"socijalna-zastita",
       "Socijalna zaštita":"socijalna-zastita", 
       "Zdravstvo":"zdravstvo", 
       "Poljoprivreda, šumarstvo i veterinarstvo":"poljoprivreda", 
       "Komunalne usluge i vodno gospodarstvo":"komunalne-usluge",
       "Zaštita okoliša i održivi razvoj":"okolis", 
       "Regionalni razvoj":"regionalni-razvoj",
       "Turizam":"turizam",
       "Statistika i informacijsko-dokumentacijska djelatnost":"statistika-informatika-dokumentacija",
       "Hidrometeorološka djelatnost":"hidrometeorologija",
       "Ostalo - neklasificirane djelatnosti":"ostalo"
       })

allData = allData.reset_index();

allData = allData.drop_duplicates(subset=['vat_number'], keep='first')
mask = allData['status'].str.contains(('updated'), na=True)
allData.loc[mask, 'tag_string'] = allData[['legal_status','founder','topics','auto','oib_tag','extra_tag']].apply(lambda x: None if x.isnull().all() else ' '.join(x.dropna()), axis=1)


# Creating auto created tag

# making name column from entity_name and city seprated by comma ','

allData["#name"] = allData[['entity_name','city']].apply(lambda x: None if x.isnull().all() else ', '.join(x.dropna()), axis=1)


# Refining emails
allData['#name'] = allData['#name'].fillna(allData['name'])
allData['#name'] = allData['#name'].fillna(allData['entity_name'])
        
allData['request_email'] = allData.request_email.fillna(allData.foi_officer_email)
allData['request_email'] = allData.request_email.fillna(allData.email)
allData['request_email'] = allData.request_email.fillna(allData.transfer_mail)

# change website to home_page
allData['home_page'] = allData.home_page.fillna(allData['website'])
allData['home_page'] = allData.home_page.fillna(allData.transfer_web)



# remove dublicate if exist 

allData = allData.drop_duplicates(subset=['vat_number'], keep='first')

# remove columns

allData = allData.drop({'extra_tag','oib_tag','transfer_web' , 'transfer_mail','name','#id','notes','publication_scheme','website','foi_officer_email','email','entity_name','postal_address','auto','status','vat_number','zip_code','city','telephone','telefax','foi_officer_name','foi_officer_telephone','founder','legal_status','topics','last_updated'}, 1)


# Export to CSV
allData = allData.reindex(columns=['#name', 'request_email','home_page','tag_string'])
                        
allData.to_csv("Data/NewData.csv", sep=',', encoding='utf-8' , index=False)


# POst on server  [Un comment 2 rows to post on Server]

#import requests
# SCHLOSSY - update host to source, fix ip_address, 
#datastr = '{"short_message":"Public authorities update job (full)", "source":"imamopravoznati", "_ip_address":"imamopravoznati.org", "facility":" imamopravoznati-vps", "_code":"200","_auto_created":"' + str(len(new.index)) +'", "_auto_updated":"'+ str(len(updated.index)) +'","_auto_removed":"'+ str(len(removed.index)) +'", "_auto_total":"'+ str(len(allData.index)) +'", "_process_name":"python", "application_name":"scrappy-updater"}'
# SCHLOSSY - uncomment response & update Graylog server:
# ERROR with REQUESTS ! ! ! ! - must be fixed
#response = requests.post('http://oprah.codeforcroatia.org:12201/gelf', data=datastr)

# conn = sqlite3.connect("data.sqlite")

# conn.execute("CREATE TABLE if not exists data ('entity_name', 'vat_number', 'postal_address', 'zip_code', 'city', 'telephone', 'telefax','website', 'email', 'foi_officer_name', 'foi_officer_telephone','foi_officer_email', 'founder', 'legal_status', 'topics','last_updated')")

# base_data.to_sql("data", conn, if_exists='replace', index=False)