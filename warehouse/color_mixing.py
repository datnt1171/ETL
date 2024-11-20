# -*- coding: utf-8 -*-
"""
Created on Sat Sep 21 08:22:40 2024

@author: KT1
"""

import pandas as pd
import numpy as np
df = pd.read_excel(r"ERP_Hao_Hut.xlsx")
df.dropna(inplace=True)

df.columns = ['order_code','product_code','product_name','qc','start_date',
              'complete_date','temp_quantity', 'estimated_quantity',
              'actual_quantity', 'loss_rate', 'loss_quantity', 'segment']

df['start_date'] = pd.to_datetime(df['start_date'], dayfirst = True)
df['complete_date'] = pd.to_datetime(df['complete_date'], dayfirst = True)

df['start_date'] = df['start_date'].dt.date
df['complete_date'] = df['complete_date'].dt.date

df['first_4_order_code'] = df['order_code']
df['first_4_order_code'] = df['first_4_order_code'].str.split("-")
df['first_4_order_code'] = df['first_4_order_code'].apply(lambda x: x[0])
df = df[df['first_4_order_code'].isin(['5102','5202'])]

df['product_code_split'] = df['product_code'].str.split("-")
df['category'] = df['product_code_split'].apply(lambda x: x[0])
df['sub_category_1'] = df['product_code_split'].apply(lambda x: x[1])
df['sub_category_2'] = df['product_code_split'].apply(lambda x: x[2])

SG_filter = ['F','C','CDNC']
SH_filter = ['CAAR','H','CKNC','CLNC','CAPU','CAPA']
df['paint_type'] = np.where(df['category'].isin(SG_filter), 'SG',
                            np.where(df['category'].isin(SH_filter), 'SH', 'Other'))  #Paint_type Wooden and steam

df.loc[df['sub_category_1'] == "CAKN", 'paint_type'] = "Silver" #Paint_type Silver

df['qc_num'] = df['qc'].str.extract(r'(\d+)')
df['qc_num'] = df['qc_num'].astype(int)
df['redundant'] = df['estimated_quantity'] % df['qc_num']

df.drop_duplicates(subset='order_code', inplace=True)

df.to_excel(r'color_mixing.xlsx', index=False)






