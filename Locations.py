from RandOpt_HastaneBilgileri import VeritabaniBaglanti
import pandas as pd
"""
Created on Wed Dec 23 02:37:13 pm 2020
@author         :   Anar Baylarov
@description    :   Konum bilgileri.
"""

class Location:
    def __init__(self, kurum):
        self.kurum = kurum.upper()

    def konum_bilgisi(self, baslangic_tarihi, bitis_tarihi):
        query = """
            SELECT [Id],[CreateDate],[Latitude],[Longitude],[Address] 
            FROM dbo.source_table 
            WHERE CAST([CreateDate] AS DATE) BETWEEN '{}' AND '{}'
        """.format(baslangic_tarihi, bitis_tarihi)
        hospitalConn = VeritabaniBaglanti('konum', 's').baglanti_al()
        with hospitalConn as conn:
            dataset = pd.read_sql_query(query, conn)
            return dataset