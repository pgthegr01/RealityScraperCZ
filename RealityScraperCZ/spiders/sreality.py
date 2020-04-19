# -*- coding: utf-8 -*-
import scrapy
import mysql.connector
import datetime

from mysql.connector import Error
from mysql.connector import errorcode

from scrapy_splash import SplashRequest


class SrealitySpider(scrapy.Spider):
    name = 'sreality'
    allowed_domains = ['www.sreality.cz']
    base_url = 'https://www.sreality.cz'
    start_urls = ['https://www.sreality.cz/hledani/prodej/byty',
                  'https://www.sreality.cz/hledani/prodej/domy',
                  'https://www.sreality.cz/hledani/prodej/pozemky']
    param_labels = []  # This is to get all the param labels.
    with open('pass.txt', 'r') as file:
        data = file.read().replace('\n', '')
    dbhost = '192.168.0.50'
    dbdatabase = 'sreality_cz'
    dbuser = 'sreality'
    dbpassword = data

    def sql_query(self, query):
        try:
            connection = mysql.connector.connect(host=self.dbhost,
                                                 database=self.dbdatabase,
                                                 user=self.dbuser,
                                                 password=self.dbpassword)
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()

        except mysql.connector.Error as error:
            print("Failed to query record {}".format(error))

        finally:
            if (connection.is_connected()):
                connection.close()
                # print("MySQL connection is closed")
        return result

    def sql_insert(self, query):
        try:
            connection = mysql.connector.connect(host=self.dbhost,
                                                 database=self.dbdatabase,
                                                 user=self.dbuser,
                                                 password=self.dbpassword)
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()
            # result = cursor.fetchall()
            cursor.close()

        except mysql.connector.Error as error:
            print("Failed to insert record into table {}".format(error))

        finally:
            if (connection.is_connected()):
                connection.close()
                # print("MySQL connection is closed")
        return

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(url=url, callback=self.parse_ids, args={"wait": 10})

    def parse_ids(self, response):
        for czprop in response.css('div.property.ng-scope'):
            itemurl = czprop.css('a').attrib['href']

            # Now we parse the property page
            self.propurl = self.base_url + itemurl
            yield SplashRequest(url=self.propurl, callback=self.parse_props, args={"wait": 10})

        # If there is a next page lets continue...
        next_page_partial_url = response.css('a.btn-paging-pn.icof.icon-arr-right.paging-next').attrib['href']
        url = self.base_url + next_page_partial_url
        yield SplashRequest(url=url, callback=self.parse_ids, args={"wait": 10})

    def parse_props(self, response):
        prop_dict = {}
        prop_dict['prop_url'] = response.url
        prop_dict['sreality_id'] = response.url.split("/")[-1]
        prop_dict['prop_location3'] = response.url.split("/")[-2]
        prop_dict['prop_title'] = response.css('div.property-title').css('span.name.ng-binding::text').get()
        prop_dict['prop_location'] = response.css('div.property-title').css('span.location::text').get()
        prop_dict['prop_price'] = response.css('div.property-title').css('span.norm-price.ng-binding::text'
                                                                         ).get().replace('\xa0', '').replace('Kč', '')
        prop_dict['prop_energy_efficiency'] = response.css('div.property-title').css(
            'span.energy-efficiency-rating__type.ng-binding::text').get()

        prop_desc = ','.join(
            response.xpath('//*[@id="page-layout"]/div[2]/div[2]/div[4]/div/div/div/div/div[5]/p').getall())
        prop_dict['prop_desc'] = prop_desc.replace('''</p>''', ''
                                                   ).replace('''<p>''', ''
                                                             ).replace('''\xa0''', ''
                                                                       ).replace('"',''
                                                                                 ).replace('<br>', '')
        prop_dict['Celková cena:'] = ''
        prop_dict['ID:'] = ''
        prop_dict['Poznámka k ceně:'] = ''
        prop_dict['ID zakázky:'] = ''
        prop_dict['Aktualizace:'] = ''
        prop_dict['Stavba:'] = ''
        prop_dict['Stav objektu:'] = ''
        prop_dict['Vlastnictví:'] = ''
        prop_dict['Podlaží:'] = ''
        prop_dict['Užitná plocha:'] = ''
        prop_dict['Plocha podlahová:'] = ''
        prop_dict['Sklep:'] = ''
        prop_dict['Energetická náročnost budovy:'] = ''
        prop_dict['Umístění objektu:'] = ''
        prop_dict['Lodžie:'] = ''
        prop_dict['Plyn:'] = ''
        prop_dict['Telekomunikace:'] = ''
        prop_dict['Elektřina:'] = ''
        prop_dict['Doprava:'] = ''
        prop_dict['Bezbariérový:'] = ''
        prop_dict['Balkón:'] = ''
        prop_dict['Datum nastěhování:'] = ''
        prop_dict['Topení:'] = ''
        prop_dict['Odpad:'] = ''
        prop_dict['Komunikace:'] = ''
        prop_dict['Vybavení:'] = ''
        prop_dict['Výtah:'] = ''
        prop_dict['Parkování:'] = ''
        prop_dict['Terasa:'] = ''
        prop_dict['Rok kolaudace:'] = ''
        prop_dict['Průkaz energetické náročnosti budovy:'] = ''
        prop_dict['Výška stropu:'] = ''
        prop_dict['Voda:'] = ''
        prop_dict['Ukazatel energetické náročnosti budovy:'] = ''
        prop_dict['Rok rekonstrukce:'] = ''
        prop_dict['Náklady na bydlení:'] = ''
        prop_dict['Garáž:'] = ''

        elems = response.css('li.param.ng-scope')
        for ul in elems:
            prop_dict[ul.css('label.param-label::text').get()] = ul.css('span.ng-binding::text').get()
            if ul.css('label.param-label::text').get() not in self.param_labels:
                self.param_labels.append(ul.css('label.param-label::text').get())

        now = datetime.datetime.utcnow()
        sql_date = now.strftime('%Y-%m-%d')
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        sql_date_yesterday = yesterday.strftime('%Y-%m-%d')

        if prop_dict['Aktualizace:'] == 'Dnes':
            prop_dict['Aktualizace:'] = sql_date
        elif prop_dict['Aktualizace:'] == 'V?era':
            prop_dict['Aktualizace:'] = sql_date_yesterday

        query_advert = 'SELECT advert_id FROM advert_tbl WHERE sreality_id="{}";'.format(prop_dict['sreality_id'])

        query_advert_entry = ('''SELECT entry_id FROM advert_entry_tbl WHERE ''' +
                              '''advert_tbl_advert_id="{}" AND entry_update="{}";'''.format(
                                  prop_dict['sreality_id'],
                                  sql_date
                              )
                              )

        insert_advert_tbl = ('''INSERT INTO advert_tbl (sreality_id, submission_date) VALUES ({},"{}")'''.format(
            prop_dict['sreality_id'], sql_date))

        insert_advert_entry_tbl = ('''INSERT INTO advert_entry_tbl ({},{},{},{},{},'''.format(
            'title', 'location2', 'price2', 'energy_efficency', 'description'
        ) + '''{},{},{},{},{},'''.format(
            'sreality_id2', 'submission_date', 'price', 'price_note', 'order_id'
        ) + '''{},{},{},{},{},'''.format(
            'entry_update', 'construction', 'object_status', 'property', 'story',
        ) + '''{},{},{},{},{},'''.format(
            'useable_area', 'floor_area', 'cellar', 'energy_performance_building', 'location',
        ) + '''{},{},{},{},{},'''.format(
            'loggia', 'gas', 'telecommunication', 'electricity', 'transport',
        ) + '''{},{},{},{},{},'''.format(
            'barrier_free', 'balcony', 'move_in_date', 'heating', 'waste',
        ) + '''{},{},{},{},{},'''.format(
            'communication', 'equipment', 'lift', 'parking', 'terrece',
        ) + '''{},{},{},{},{},'''.format(
            'acceptance_year', 'building_energy_performance_certificate', 'ceiling_height', 'water',
            'building_performance_indicator',
        ) + '''{},{},{},{},{},'''.format(
            'year_of_reconstruction', 'housing_cost', 'garage', 'url', 'location3',
        ) + '''{})'''.format(
            'advert_tbl_advert_id'
        ) + ''' VALUES ("{}","{}","{}","{}","{}",'''.format(
            prop_dict['prop_title'], prop_dict['prop_location'],
            prop_dict['Celková cena:'].replace('\xa0', '').replace('Kč', '').replace('za nemovitost','').replace(' ',''),
            prop_dict['prop_energy_efficiency'], prop_dict['prop_desc'],
        ) + '''"{}","{}","{}","{}","{}",'''.format(
            prop_dict['ID:'], now.strftime('%Y-%m-%d %H:%M:%S'), prop_dict['prop_price'],
            prop_dict['Poznámka k ceně:'], prop_dict['ID zakázky:'],
        ) + '''"{}","{}","{}","{}","{}",'''.format(
            prop_dict['Aktualizace:'], prop_dict['Stavba:'], prop_dict['Stav objektu:'],
            prop_dict['Vlastnictví:'], prop_dict['Podlaží:'],
        ) + '''"{}","{}","{}","{}","{}",'''.format(
            prop_dict['Užitná plocha:'], prop_dict['Plocha podlahová:'], prop_dict['Sklep:'],
            prop_dict['Energetická náročnost budovy:'], prop_dict['Umístění objektu:'],
        ) + '''"{}","{}","{}","{}","{}",'''.format(
            prop_dict['Lodžie:'], prop_dict['Plyn:'], prop_dict['Telekomunikace:'],
            prop_dict['Elektřina:'], prop_dict['Doprava:'],
        ) + '''"{}","{}","{}","{}","{}",'''.format(
            prop_dict['Bezbariérový:'], prop_dict['Balkón:'], prop_dict['Datum nastěhování:'],
            prop_dict['Topení:'], prop_dict['Odpad:'],
        ) + '''"{}","{}","{}","{}","{}",'''.format(
            prop_dict['Komunikace:'], prop_dict['Vybavení:'], prop_dict['Výtah:'],
            prop_dict['Parkování:'], prop_dict['Terasa:'],
        ) + '''"{}","{}","{}","{}","{}",'''.format(
            prop_dict['Rok kolaudace:'], prop_dict['Průkaz energetické náročnosti budovy:'],
            prop_dict['Výška stropu:'], prop_dict['Voda:'], prop_dict['Ukazatel energetické náročnosti budovy:'],
        ) + '''"{}","{}","{}","{}", "{}",'''.format(
            prop_dict['Rok rekonstrukce:'], prop_dict['Náklady na bydlení:'].replace('"',''),
            prop_dict['Garáž:'], prop_dict['prop_url'], prop_dict['prop_location3'],
        ) + '''"{}");'''
                                   )
        query_advert_result = self.sql_query(query_advert)

        if not query_advert_result:
            self.sql_insert(insert_advert_tbl)
            result = self.sql_query(query_advert)
            self.sql_insert(insert_advert_entry_tbl.format(query_advert_result[0][0]))
            print("New Advert found: {}".format(prop_dict['sreality_id']))
        else:
            query_advert_entry_result = self.sql_query(query_advert_entry)
            if not query_advert_entry_result:
                self.sql_insert(insert_advert_entry_tbl.format(query_advert_result[0][0]))
                print("New Advert Update: {}".format(prop_dict['sreality_id']))
        prop_dict.clear()

        yield


