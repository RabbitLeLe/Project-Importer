#-*- coding: utf-8 -*-
try:
    from configparser import ConfigParser
    from tqdm import tqdm
    import sqlite3
    import requests
    import mysql.connector
    import sys, os
    import time, datetime
except:
    exit('[!] no required modules')

def program_log(log_text):
    event_time = str(datetime.datetime.now().replace(microsecond=0))
    log_file = open('log_file.log', "a")
    log_file.write(event_time + ';' + log_text + '\n')
    log_file.close()

def database_file(start):
    db_file = ''
    if start == False:
        try:
            for file in os.listdir(home_path):
                if '.db' in file:
                    db_file = file
                    start = True
                    break
        except:
            exit()
    else:
        pass
    if db_file == '':
        print('No .db files in directory.')
        input()
        exit()
    return db_file

def database_config():
    config = {
        'host': '',
        'database': 'Project',
        'user': '',
        'password': '',
        'charset': 'utf8',
        'use_unicode': True,
        'get_warnings': True,
    }
    return config

def database_connect(config):
    try:
        con = mysql.connector.connect(**config)
        cur = con.cursor()
    except:
        print('[!] Database connection error!')
    return con, cur

def database_insert(cur, db_file):
    NIP = ''
    VAT = ''

    con2 = sqlite3.Connection(db_file)
    con2.row_factory = sqlite3.Row
    cur2 = con2.cursor()
    cur2.execute ('SELECT max(id) FROM link_data')
    for row in cur2.fetchall():
        last_id = str((dict(row)))
        last_id = last_id.replace("{'max(id)': ", "").replace("}", "")
    cur2.execute('SELECT * FROM link_data as ld, adv_data as ad, seller_data as sd where ld.id=ad.KeyId and ld.id=sd.KeyId and ld.id>'+last_element)
    try:
        for row in cur2.fetchall():
            seller_id = ''
            adv_id = ''
            seller_dir = ''
            date1 = ''
            date2 = ''
            url = ''
            adv_title = ''
            price = ''
            currency = ''
            user_name = ''
            category = ''
            location = ''
            region = ''
            subregion = ''
            contact = ''
            phone = ''
            text = ''

            data = str((dict(row)))
            data = data.replace('\\r', '').replace('\\', '')
            data = data.replace('"', "'")
            data = data.split("', '")
            for item in data:
                item = item.replace("'", "")
                if 'id' in item and 'adv_id' in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    try:
                        idid = (item[1])
                        idid = idid[:-15]
                        print('0.id: ' + idid)

                        adv_id = (item[2])
                        print('1.adv_id: ' + adv_id)
                        continue
                    except:
                        program_log('[!] No id or adv_id: ERROR')
                        break

                if 'url' in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    try:
                        url = (item[1])
                        print('2.url: ' + url)
                        continue
                    except:
                        print('2.url: Except')
                        pass

                if 'date1' in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    try:
                        date1 = (item[1])
                        print('3.date1: ' + date1)
                        continue
                    except:
                        print('3.date1: Except')
                        pass

                if 'date2' in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    try:
                        date2 = (item[1])
                        print('4.date2: ' + date2)
                        continue
                    except:
                        print('4.date2: Except')
                        pass

                if 'adv_title' in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    try:
                        adv_title = (item[1])
                        adv_title = adv_title.replace("'", " ")
                        adv_title = adv_title.replace('"', '')
                        print('5.adv_title: ' + adv_title)
                        continue
                    except:
                        print('5.adv_title: Except')
                        pass

                if 'category' in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    try:
                        category = (item[1])
                        print('6.category: ' + category)
                        continue
                    except:
                        print('6.category: Except')
                        pass

                if 'text' in data[8] and text =='':
                    item = data[8].replace("'", "")
                    try:
                        text = item[5:]
                        text = ' '.join(text.split())
                        text = text.replace('&oacute;', 'ó').replace('\u003cp', '')
                        text = text.replace('\u003cbr', '').replace('\u003e', '')
                        text = text.replace('\u003cbr', '').replace('\u003e', '')
                        print('7.text: OK')
                        continue
                    except:
                        print('7.text: Except')
                        pass

                if 'price' in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    if len(item[0]) > 4:
                        pass
                    else:
                        try:
                            price = (item[1])
                            print('8.price: ' + price)
                            continue
                        except:
                            print('8.price: Except')
                            pass

                if 'currency' in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    try:
                        currency = (item[1])
                        print('9.currency: ' + currency)
                        continue
                    except:
                        print('9.currency: Except')
                        pass

                if 'location' in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    try:
                        location = (item[1])
                        print('10.location: ' + location)
                        continue
                    except:
                        print('10.location: Except')
                        pass

                if "region" in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    if len(item[0]) > 6:
                        pass
                    else:
                        try:
                            region = (item[1])
                            print('11.region: ' + region)
                            continue
                        except:
                            print('11.region: Except')
                            pass

                if 'subregion' in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    try:
                        subregion = (item[1])
                        print('12.subregion: ' + subregion)
                        continue
                    except:
                        print('12.subregion: Except')
                        pass

                if 'seller_id' in item and 'seller_dir' in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    try:
                        seller_id = (item[2])
                        seller_id = seller_id[:-12]
                        print('13.seller_id: ' + seller_id)
                    except:
                        print('13.seller_id: Except')
                        pass

                    try:
                        seller_dir = (item[3])
                        #seller_dir = seller_dir[1:]
                        print('14.seller_dir: ' + seller_dir)
                        continue
                    except:
                        print('14.seller_dir: Except')
                        pass

                if 'user_name' in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    try:
                        user_name = (item[1])
                        print('15.user_name: ' + user_name)
                        continue
                    except:
                        print('15.user_name: Except')
                        pass

                if 'phone' in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    try:
                        phone = (item[1])
                        print('16.phone: ' + phone)
                        continue
                    except:
                        print('16.phone: Except')
                        pass

                if 'contact' in item:
                    try:
                        item = item.split(': ')
                    except:
                        pass
                    try:
                        contact = (item[1])
                        print('17.contact: ' + contact)
                        continue
                    except:
                        print('17.contact: Except')
                        pass
            try:
                try:
                    print(idid + '/' + last_id)
                except:
                    pass
                cur.execute("INSERT INTO Project(seller_id, adv_id, seller_dir, date1, date2, url, adv_title, price, currency, user_name, category, location, region, subregion, contact, phone, text) VALUES ('" + seller_id + "', '" + adv_id + "', '" + seller_dir + "', '" + date1 + "', '" + date2 + "', '" + url + "', '" + adv_title + "', '" + price + "', '" + currency + "', '" + user_name + "', '" + category + "', '" + location + "', '" + region + "', '" + subregion + "', '" + contact + "', '" + phone + "', '" + text + "')")
            except:
                try:
                    cur.execute("INSERT INTO Project(seller_id, adv_id, seller_dir, date1, date2, url, adv_title, price, currency, user_name, category, location, region, subregion, contact, phone, text) VALUES ('" + seller_id + "', '" + adv_id + "', '" + seller_dir + "', '" + date1 + "', '" + date2 + "', '" + url + "', '" + adv_title + "', '" + price + "', '" + currency + "', '" + user_name + "', '" + category + "', '" + location + "', '" + region + "', '" + subregion + "', '" + contact + "', '" + phone + "', '" + text + "')")
                except:
                    program_log('[!] INSERT ERROR:' + idid + ';' + adv_id + ';' + url + ';' + date1)
                    pass
        con2.close()
        con.close()
        parser['database']['last'] = idid
        with open('settings//config_file.ini', 'w') as configfile:
            parser.write(configfile)
            configfile.close()
        exit()
    except:
        program_log('[!] Unknown ERROR!!!!!!!!')
        con2.close()
        con.close()
        try:
            parser['database']['last'] = idid
            with open('settings//config_file.ini', 'w') as configfile:
                parser.write(configfile)
                configfile.close()
        except:
            program_log('[!] check LAST')
        exit()


parser = ConfigParser()
parser.read('settings//config_file.ini')
home_path = parser.get('directories','home')
settings_path = parser.get('directories','settings')
last_element = parser.get('database','last')

start = False
db_file = database_file(start)
config = database_config()
con, cur = database_connect(config)
print('File to upload: ' + db_file)
enter = input('Press ENTER if OK:')
if enter == '':
    database_insert(cur, db_file)
else:
    exit()
input('Press ENTER to exit.')
