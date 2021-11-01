"""
Created on Wed Dec 23 02:37:13 pm 2020
@author         :   Anar Baylarov
@description    :   Veritabanı bağlantı bilgileri
"""

import logging
import warnings
import pyodbc
import time as tm
import urllib
import sqlalchemy as sal
import sys
import cx_Oracle as oracle
import base64
import platform

warnings.filterwarnings("ignore")
logging.basicConfig(filename='loglar.log', filemode='a', format='%(asctime)s %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)


class company:
    def __init__(self, company_ismi):
        self.company_ismi = company_ismi.upper()

    _company_listesi = ['company_1', 'company_2', 'company_3']

    __company_bilgileri = {'company_1': {'server': r'1.1.1.1\OLTP', 'code': 'CMP1', 'isTr': False, 'partition': '11'},
                           'company_2': {'server': r'2.2.2.2\OLTP', 'code': 'CMP2', 'isTr': False,
                                        'partition': '12'},
                           'company_3': {'server': r'3.3.3.3\PUSULA', 'namenode': '10.10.10.10',
                                             'db': 'dbo', 'code': 'CMP3', 'isTr': True,
                                             'partition': '13'},
                           'location': {'server': r'4.4.4.4\ONLINE', 'db': 'dbo', 'isTr': False},
                           'dwh': {'server': r'5.5.5.5', 'db': 'dbo', 'port': '8888',
                                   'isTr': False},
                           'erp': {'server': r'6.6.6.6', 'db': 'dbo', 'port': '8888',
                                   'isTr': False}}

    @classmethod
    def company_listesi(cls):
        return cls._company_listesi

    @classmethod
    def company_kodu(cls, company_name):
        return cls.__company_bilgileri[company_name.upper()]['code']

    def __company_bilgisi(self, company_name):
        return self.__company_bilgileri[company_name]

    @classmethod
    def company_partition(cls, company_name):
        partition = 'P{}'.format(cls.__company_bilgileri[company_name.upper()]['partition'])
        return partition

    def _kaynak_db_bilgileri(self):
        company = self.company_ismi
        try:
            company_bilgi = self.__company_bilgisi(self.company_ismi)
            if company_bilgi['isTr']:
                return {'db': company_bilgi['db'], 'user': 'user1', 'psw': 'Q2FTMQ==', 'tcon': 'No',
                        'server': company_bilgi['server']}
            else:
                if self.company_ismi == 'KONUM':
                    return {'db': company_bilgi['db'], 'user': 'user2', 'psw': 'eVjhWm4=', 'tcon': 'No',
                            'server': company_bilgi['server']}
                elif self.company_ismi == 'dwh':
                    return {'db': company_bilgi['db'], 'user': 'user3', 'psw': 'YmghK==', 'tcon': 'No',
                            'server': company_bilgi['server'], 'port': company_bilgi['port']}
                elif self.company_ismi == 'erp':
                    return {'db': company_bilgi['db'], 'user': 'user4', 'psw': 'eBNzY=', 'tcon': 'No',
                            'server': company_bilgi['server'], 'port': company_bilgi['port']}
                else:
                    return {'db': 'comp', 'user': 'mp_dev', 'psw': 'QDa0TxQ=', 'tcon': 'No',
                            'server': company_bilgi['server']}
        except (KeyError) as error:
            logging.error(
                "Belirlenen şirket ismi listede bulunmuyor. Detay: \n{}".format(
                    company, error))
            sys.exit()

    def _hedef_db_bilgileri(self):
        server = self.__company_bilgileri['CMP125']['server']
        return {'db': 'comp', 'user': 'user125', 'psw': 'Q2==TLQ', 'tcon': 'No', 'server': server}


class VeritabaniBaglanti(company):
    '''
    + ve - bağlantı oluşturur.
    s -> source         veritabanları erişimi
    t -> target         Hedef veritabanı
    e -> evaluation     Değerlendirme işlemleri için bağlantı
    '''
    pyodbc.pooling = False
    baglanti_tipi_aciklama = {"s": "Source", "t": "Target", "e": "Evaluation"}

    def __init__(self, kurum_ismi, baglanti_tipi):
        super().__init__(kurum_ismi)
        self.baglanti_tipi = baglanti_tipi

    def __db_baglanti(self):
        global port, db_bilgileri
        oracle_dbs=['DWH', 'ERP']
        if self.baglanti_tipi == 'e':
            psw = base64.b64decode('Q2GT=xQ'.encode('ascii')).decode('ascii')
            params = urllib.parse.quote_plus("DRIVER={SQL Server};"
                                             r"SERVER=5.5.5.5\OLTP;"
                                             "DATABASE=comp;"
                                             "UID=usr;"
                                             "Trusted_Connection=no;"
                                             f"PWD={psw}")

            engine = sal.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
            __conn = engine.connect()
        else:
            if self.baglanti_tipi == 's':
                db_bilgileri = self._kaynak_db_bilgileri()
            elif self.baglanti_tipi == 't':
                db_bilgileri = self._hedef_db_bilgileri()

            server, db, user, psw, tcon = db_bilgileri['server'], db_bilgileri['db'], db_bilgileri['user'], \
                                          base64.b64decode(db_bilgileri['psw'].encode('ascii')).decode(
                                              'ascii'), db_bilgileri['tcon']

            try:
                port = db_bilgileri['port']
            except KeyError:
                pass

            if self.company_ismi in oracle_dbs:
                dir_map = {"Windows": r"C:\oracle\instantclient", "Linux": r"/opt/oracle/instantclient"}
                oracle_client_dir = dir_map[platform.system()]
                oracle.init_oracle_client(lib_dir=oracle_client_dir)

                tns = oracle.makedsn(server, port=port, service_name=db)
                __conn = oracle.connect(user=user, password=psw, dsn=tns, encoding="UTF-8")
            else:
                __conn = pyodbc.connect(driver='{SQL Server}', host=server, database=db, trusted_connection=tcon,
                                        user=user,
                                        password=psw, autocommit=True)
        return __conn

    def baglanti_al(self):
        company = self.company_ismi
        baglanti_tipi = self.baglanti_tipi_aciklama[self.baglanti_tipi]

        calisma_sayi = 3
        bekleme_suresi = 180
        tekrar_denesin_mi = True
        deneme_sayi = 0

        while tekrar_denesin_mi and deneme_sayi < calisma_sayi:
            try:
                cnxn = self.__db_baglanti()
                tekrar_denesin_mi = False
                logging.info(
                    "SQL Server-Python connection as {} is success.".format(company,
                                                                                                    baglanti_tipi))
                return cnxn
            except(Exception) as error:
                logging.error("Error occured while SQL Server-Python connection as {}.\n"
                              "Retrying SQL Server connection after {} seconds. More: \n{}".format(company,
                                                                                                   baglanti_tipi,
                                                                                                   bekleme_suresi,
                                                                                                   error))
                deneme_sayi += 1
                tm.sleep(bekleme_suresi)

    def __enter__(self):
        return self.cnxn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cnxn.close()


