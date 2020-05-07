# Last Modified : 2019.12.9 23:10
import pandas as pd
from os import listdir
from datetime import datetime

file_format = 'MUpload'
date_today  = datetime.today().strftime('%Y%m%d')

# Write Log
f = open(r""+'Processing_'+date_today+'_log.txt', 'a') # open file
f.write('\nBegin : '+datetime.today().strftime('%Y%m%d_%H%M%S')+'\n') # begin time

try :
    # Mapping Data From Mixpanel JQL
    MixpanelMappingTable = pd.read_csv('Mkt_InviteCodeMapping_Table.csv') # read
    MixpanelMappingTable.columns = ['invite_code','distinct_id','value'] # rename column
    # print(MixpanelMappingTable)
    
    # Read Csv File
    CSV_file = list(filter(lambda y: y.startswith(file_format), list(filter(lambda x: '.csv' in x, listdir())))) # get csv file names
    
    for EachFile in CSV_file :
        # push list
        RawPushList = pd.read_csv(EachFile) 
        # print(RawPushList)
        
        # check if column name correct
        if list(RawPushList.columns) == ['invite_code', 'Notification_EXP'] :
            
            f.write('< '+EachFile+' >\n')
            
            # Aggr Notification_EXP Before Mapping
            Aggr_RawPushList = RawPushList['Notification_EXP'].value_counts().reset_index()
            Aggr_RawPushList.columns = ['Notification_EXP', 'count']
            Aggr_RawPushList.sort_values(by=['Notification_EXP'], inplace=True)
            # Aggr_RawPushList.to_csv(r""+'Processing_'+date_today+'_log.txt', index=False, sep=' ', mode='a') # write log
            f.write('@Before Mapping\n'+Aggr_RawPushList.to_csv(index=False, sep='\t')) # write log
             
            # merge
            Tmp_UploadMixpanelList = pd.merge(MixpanelMappingTable, RawPushList, on="invite_code", how="inner")
            # print(UploadMixpanelList)
            
            # select columns
            UploadMixpanelList = Tmp_UploadMixpanelList[['distinct_id', 'Notification_EXP']]
            
            # mr member list
            append_mr_mbr = pd.DataFrame({
            'distinct_id' : [
              'BPMEEW21', # Jerry
              'BYDNWS62', # Mavix
              'BVTEXQ52', # Sarah
              'BWPMHN25', # Vern
              'CPPXNQ56', # Emily
              'BA8891C0-AB6E-47D5-81FC-234E2E530FA9', # BRAXJB53 Cinny
              'FDBC1AEF-036C-46C0-9914-178CC7E05E2F', # CNXLVL56 Debra
              '0D4A0C91-8922-4546-8A08-F327003CD840', # CGHVGG51 Abbie
              '3e89c575-a2ec-4976-a474-5cba9b8b00bf', # BXHRWS58 Trista  
              'cf8e11a6-fd77-4b9d-b627-4460ca490616', # VDRW8835 Leo
              'f0316d53-42d2-41f7-aded-10753df7cc82', # BXGCHJ62 Fion
              '02f1c983-db54-4fe2-869f-6735d9320325', # BXHPAS58 Serene
              'BQADGT23', # Chia Liang
              'BXEXWB39', # Ryder
              'ae5397f5-a17a-42f0-b128-10c5ee06e6f3'  # BQNZRG23 Virgil 
            ],
            'Notification_EXP' : UploadMixpanelList['Notification_EXP'].iloc[0]})
            # print(append_mr_mbr)
            
            # append data
            UploadMixpanelList = UploadMixpanelList.append(append_mr_mbr)
            UploadMixpanelList = UploadMixpanelList.reset_index(drop=True) # reset index
            # print(UploadMixpanelList)
            
            Unique_UploadMixpanelList = UploadMixpanelList.drop_duplicates() # keep unique rows
            
            # Aggr Notification_EXP After Mapping
            Aggr_Unique_UploadMixpanelList = Unique_UploadMixpanelList['Notification_EXP'].value_counts().reset_index()
            Aggr_Unique_UploadMixpanelList.columns = ['Notification_EXP', 'count']
            Aggr_Unique_UploadMixpanelList.sort_values(by=['Notification_EXP'], inplace=True)
            # Aggr_Unique_UploadMixpanelList.to_csv(r""+'Processing_'+date_today+'_log.txt', index=False, sep=' ', mode='a') # write log
            f.write('@After Mapping\n'+Aggr_Unique_UploadMixpanelList.to_csv(index=False, sep='\t')+'\n')
            
            Unique_UploadMixpanelList.to_csv(r""+EachFile[:-4]+'_'+date_today+'.csv', sep=',', index=False, header=False) # write
    # clean env
    del MixpanelMappingTable
except :
    f.write('Opps\n')

f.write('End : '+datetime.today().strftime('%Y%m%d_%H%M%S')) # end time
f.close() # close file