import mysql.connector

print '''
##########################
start to generate hive sql
##########################
'''

dbConfig = {
    'user': 'select_only',
    'password': 'select_only',
    'host': '10.32.119.14',
    'database': 'hive',
    'use_unicode': True
}

# hiveDbs = ('oms', 'dop', 'prmt', 'csc', 'crm3rd'
# 	, 'emall_order', 'commodity', 'rigida', 'mrs', 'wxs'
# 	, 'crm', 'mdmuser', 'cxbdb', 'eadccc', 'grape20'
# 	, 'mdmpartner', 'society', 'ces_score_app', 'mms', 'mmstask'
# 	, 'sso', 'datacenter', 'pay_center', 'common_oms','carmall')

hiveDbs = ("mdmuser", "oms", "dop", "prmt", "csc", "crm3rd", "emall_order", "commodity", "rigida", "mrs", "wxs", "crm", "cxbdb", "eadccc", "grape20"
    , "mdmpartner", "ces_score_app", "mms", "mmstask", "sso", "pay_center", "common_oms","bi","carmall",'cl')

secretConfig = {
	"mdmuser": {"t_address": ("mobile", "address_name")	, "t_asset": ("engine_number", "vin_code")	, "t_asset_flow": ("engine_number", "vin_code", "car_license")	, "t_bankaccount": ("bank_account_number")	, "t_identity_card": ("number")	, "t_phone": ("number")	, "t_potential_user": ("mobile", "email")	, "t_vehiclet_match_info": ("mobile_phone_num", "original_vin", "oem_vin", "oem_license_plate", "oem_engine_number", "oem_user_phone", "oem_user_cert_num", "oem_user_email", "oem_user_address")	, "t_webaccount": ("number")
             },
	"oms": {
            "t_leads": ("cust_mobile_no", "cust_email")	, "t_pre_order": ("cust_email", "mobile_no")
	},
	"dop": {
            "t_dealer": ("bank_account_code")	, "t_user": ("mobile", "email")	, "t_user_meetinfo": ("mobile", "user_address", "open_id")
	},
	"prmt": {
		"t_act_order" : ("mobile_no"), "t_auction_log" : ("mobile"), "t_battle_balance" : ("open_id"), "t_battle_detail" : ("open_id", "owner_open_id")
		,"t_battle_recharge" : ("open_id", "mobile_phone"), "t_coupon" : ("mobile", "verify_code"), "t_customer_info" : ("telephone", "email", "cust_address")
    ,"t_follow_order" : ("mobile"), "t_lottery_record" : ("mobile"), "t_praise" : ("mobile"), "t_promotion_subscription" : ("mobile")
    ,"t_red_packet" : ("sender_mobile", "receiver_mobile"), "t_send_msg" : ("mobile"), "t_taxi_coupon" : ("mobile"), "t_user_infomation" : ("user_cell", "customer_address")
    ,"t_venue_remind" : ("mobile"), "t_win" : ("mobile"), "t_wx_oper_log" : ("open_id")
  },
  "csc" : {
    "t_member_import" : ("phone", "vin_code")
  },
  "crm3rd" : {
    "t_call_in_result" : ("vs_caller", "vs_called","vs_transfer"), "t_shake_result" : ("vs_shake_token", "vs_user_phone", "vs_customer_phone")
  },
  "emall_order" : {
  "t_coupon" : ("code") , "t_order" : ("mobile"), "t_order_item" : ("coupon_code")
  },
  "commodity" : {
  "t_coupon" : ("code") , "t_coupon_backup" : ("code"), "t_provider" : ("contact_mobile", "contact_phone", "contact_email")
  , "t_provider_settlement" : ("bank_account")
  },
  "rigida" : {
   "t_app_activation" : ("deviceid"), "t_promotioner" : ("email", "mobilephone"), "t_user_deviceid_app" : ("deviceid")
  },
  "mrs" : {
  "t_car_owner" : ("owner_mobile","car_vin","id_card", "email", "address")
  },
  "wxs" : {
  "sub_user_message_record" : ("openid"), "t_baiyear" : ("openid"), "t_keywordstatistics" : ("openid"), "t_peccancyinfo": ("telphone", "openid", "peccancycode")
  , "t_peccancyorderinfo" : ("peccancycode"), "t_seckillleaveinfo" : ("openid", "telphone"), "t_send_msg" : ("openid", "telphone", "wxid")
  , "t_user_message_record" : ("openid"), "t_usersubscribe" : ("openid"), "t_usersubscribe_sales" : ("openid"), "t_weixinnumber" : ("wxnumber", "appid", "secret", "token")
  , "t_wx_memberqrsequence" : ("openid"), "t_wxuserinfo" : ("openid"), "wx_active_run_info" : ("openid", "phone"), "wx_activity_info" : ("openid", "phone", "idcard")
  , "wx_oilcard_activeinfo" : ("openid", "idcard", "address_detail", "telephone")
  },
  "crm" : {
  "t_ccc_feedback" : ("cust_mobile_num"), "t_invalidate_feedback_info" : ("cust_mobile_num", "cert_no"), "t_leads_feedback" : ("cust_mobile_num", "cert_no", "vel_vin")
  ,"t_leads_feedback_his" : ("cust_mobile_num", "cert_no", "vel_vin"), "t_leads_minisite" : ("cust_mobile_num"), "t_main_leads" : ("cust_mobile_num")
  , "t_merged_leads" : ("cust_mobile_num"), "t_ob_leads" : ("cust_mobile_no"), "t_temp_leads" : ("cust_mobile_no")
  },
  "cxbdb" : {
  "t_activity_present" : ("address", "expressno", "vouchercode", "phone", "oilcardid"), "t_credit_winner" : ("phone", "phonebind"), "t_designated_driving_info" : ("userphone")
  , "t_generalize_user" : ("mobilephone"), "t_obd_info" : ("vincode"), "t_vehicle_peccancy" : ("carno")
  },
  "eadccc" : {
  "t_call_record" : ("call_no", "cust_mobile_num"), "t_it_activities_data" : ("user_mobile", "dealer_mobile"), "t_it_general_reception_data" : ("mobilephone")
  , "t_it_leads_data" : ("cust_mobile_num"), "t_it_leads_result" : ("cust_mobile_num"), "t_it_mainten_data" : ("cust_phone", "user_phone"), "t_it_mainten_result" : ("cust_phone", "user_phone")
  , "t_it_order_activities_data" : ("user_mobile"), "t_it_order_tio_data" : ("address", "mobile", "mobile_no"), "t_it_order_tio_result" : ("address", "mobile", "mobile_no")
  , "t_it_task_activities" : ("user_mobile", "user_address"), "t_it_used_car_data" : ("mobilephone"), "t_it_used_car_result" : ("mobilephone")
  , "t_mail_send" : ("to_address"), "t_sale_leads" : ("mobilephone", "fixedphone"), "t_service_record" : ("call_no"), "t_service_record_detail" : ("custphone")
  , "t_sr_worksheet" : ("account_phone", "contact_phone"), "t_task_worksheet_info" : ("mobilephone", "fixedphone")
  },
  "grape20" : {
  "t_user_coupon" : ("verifycode", "usermobilephone", "uservehicleplatenumber"), "t_user_feedback" : ("mobilephone"), "t_user_order" : ("deviceid", "vehicleplatenumber", "usermobilephone")
  ,"t_user_vehicle" : ("vehicleplatenumber")
  },
  "mdmpartner" : {
  "t_employee" : ("mobile", "email"), "t_partner" : ("bank_account_code"), "t_partner_bank_acct" : ("bank_account_code")
  },
  "ces_score_app" : {
  "t_insurance_policy" : ("car_no", "frame_no", "contact_phone", "certificate_number")
  },
  "mms" : {
  "t_asset_import" : ("phone", "license_plate", "vin_code", "engine_number", "id_card_no"), "t_asset_match_log_in" : ("mobile_phone_num", "original_vin", "oem_vin", "oem_license_plate", "oem_engine_number", "oem_user_phone", "oem_user_cert_num", "oem_user_email", "oem_user_address","oem_last_contact_phone")
  , "t_bind_appl" : ("car_license", "id_card", "owner_mobile", "vin_code", "engine_number"), "t_bind_appl_his" : ("car_license", "id_card", "owner_mobile", "vin_code", "engine_number")
  , "t_customer" : ("cert_no", "address", "telephone", "mobile", "email"), "t_invite_send" : ("email"), "t_memb_contact_other" : ("qq", "weixin", "micro_blog", "bank_acct")
  , "t_memb_delivery_addr" : ("mobile", "telephone", "email", "address"), "t_member" : ("cert_no", "address", "telephone", "mobile", "email")
  , "t_member_import" : ("phone", "idcard", "vin_code"), "t_member_sms" : ("mobile"), "t_user_record" : ("open_id", "mobile")
  , "t_vel_bind_appl" : ("owner_mobile", "vin", "id_card"), "tmp_t_bind_appl" : ("car_license", "id_card", "owner_mobile", "vin_code")
  }, 
  "mmstask" : {},
  "sso" : {},
  "pay_center" : {},
  "common_oms" : {
  "t_common_order" : ("client_mobile","client_email"), "t_coupon" : ("code"), "t_emall_order" : ("receiver_mobile", "receiver_address")
  ,"t_emall_order_rent_car_item" : ("email", "id_card"), "t_increment" : ("mobile", "address"), "t_peccancy_order" : ("order_mobile_no")
  ,"t_peccancy_order_item" : ("engine_no", "car_no", "peccancy_id", "peccancy_code"), "t_peccancy_settle_cert" : ("mobile_no")
  , "t_refund_cert" : ("cust_email", "mobile_no", "bank_account"), "t_settle_cert" : ("cust_email", "mobile_no"), "t_settle_cert_bak_17" : ("cust_email", "mobile_no")
  , "temp_t_coupon" : ("code")
  } ,
  "bi" : {
  "cx_dim_user" : ("mobile", "telephone", "email", "phone_number")
  }
}

tableCreatedDateConfig = {
	"mdmuser": {
            "t_address": "created_time"	, "t_asset": "created_time"	, "t_asset_flow": "created_time"	, "t_bankaccount": "created_time"	, "t_identity_card": "created_time"	, "t_career": "created_time"	, "t_contact": "created_time"	, "t_education": "created_time"	, "t_entity_history": "create_time"	, "t_hobby": "created_time"	, "t_identity_card": "created_time"	, "t_operation_data_log": "created_time"	, "t_phone": "created_time"	, "t_potential_user": "create_time"	, "t_user": "created_time"	, "t_user_bind": "created_time"	, "t_user_description": "created_time"	, "t_user_flow": "created_time"	, "t_user_source": "created_time"	, "t_vehiclet_match_info": "create_time"	, "t_webaccount": "created_time"	, "t_user_offline": "create_time"
	},
	"oms": {
            "t_valid_code": "create_time"	, "t_pre_order_log": "create_time"	, "t_pre_order_prmt": "create_time"	, "t_pre_order": "create_time"	, "t_oppor": "create_time"	, "t_leads": "create_time"
	},
	"dop": {
            "t_store_brand": "create_time"	, "t_point_record": "create_time"	, "t_user": "create_time"	, "t_user_meetinfo": "create_time"
	},
	"prmt" : {
	"t_battle_cardrule":"create_time"
	,"t_user_attention":"create_time"
	,"t_coupon":"create_time"
	,"t_user_infomation":"create_time"
	,"t_red_packet":"create_time"
	,"t_battle_balance":"create_time"
	,"t_gift_quota":"create_time"
	,"t_follow_order":"createtime"
	,"t_promotion_merchandise":"create_time"
	,"t_praise_info":"create_time"
	,"t_promotion_qualification":"create_time"
	,"t_promotion_subscription":"create_time"
	,"t_act_order":"create_time"
	,"t_wx_oper_log":"create_time"
	,"t_promotion":"create_time"
	,"t_customer_info":"create_time"
	},
  "csc" : {
  "t_member_import" : "create_time"
  },
  "emall_order" : {
  "t_basket":"create_time"
  ,"t_coupon":"create_time"
  ,"t_order":"create_time"
  ,"t_basket_details":"create_time"
  ,"t_order_item":"create_time"
  },
  "commodity" : {
  "t_sms_app":"row_create_time"
  ,"t_archive_log":"update_time"
  ,"t_catalog_merchandise":"update_time"
  ,"t_codetype":"update_time"
  ,"t_catalog_product_define":"update_time"
  ,"t_sms_template_info":"row_create_time"
  ,"t_stock_log":"update_time"
  ,"t_batch_request_log":"update_time"
  ,"t_batch_config":"update_time"
  ,"t_coupon_log":"update_time"
  ,"t_coupon":"update_time"
  ,"t_coupon_backup":"update_time"
  ,"t_codevalue":"update_time"
  ,"t_merchandise_item":"update_time"
  },
  "rigida" : {
  "t_app_record":"createddatetime"
  ,"t_os_record":"createddatetime"
  ,"t_user_operation_info":"createddatetime"
  ,"t_app_activation":"createddatetime"
  ,"t_user_deviceid_app" : "operatetime"
  ,"t_user_operation" : "opttime"
  },
  "mrs" : {
  "t_balance_expire":"create_time"
  ,"t_balance_bak":"create_time"
  ,"t_balance":"create_time"
  ,"t_cd_account":"create_time"
  ,"t_account":"create_time"
  ,"t_cd_thirdparty_callback":"create_time"
  ,"t_cd_exchange_rate":"create_time"
  ,"t_cd_balance":"create_time"
  ,"t_car_owner":"create_date"
  ,"t_cd_credit_delivery":"create_time"
  },
  "wxs" : {
  "wx_active_run_info":"createtime"
  ,"t_user_message_record":"createtime"
  ,"sub_sale_message_record":"createtime"
  ,"sub_user_message_record":"createtime"
  ,"t_articleinfo":"createtime"
  ,"wx_oilcard_activeinfo":"create_time"
  ,"t_baiyear" : "inserttime"
  ,"t_keywordstatistics" : "inserttime"
  ,"t_peccancyinfo" : "inserttime"
  ,"t_peccancyorderinfo" : "inserttime"
  ,"t_seckillleaveinfo" : "inserttime"
  , "t_send_msg" : "inserttime"
  ,"t_usersubscribe" : "inserttime"
  ,"t_usersubscribe_sales" : "inserttime"
  ,"t_wx_memberqrsequence" : "inserttime"
  ,"t_wxuserinfo" : "inserttime"
  ,"wx_activity_info" : "update_time"
  },
  "crm" : {
  "t_temp_leads":"source_create_time"
  ,"t_ob_leads":"source_create_time"
  ,"t_leads_feedback":"create_time"
  ,"t_leads_feedback_his":"create_time"
  ,"t_invalidate_feedback_info":"create_time"
  ,"t_invalid_leads":"create_time"
  ,"t_main_leads":"create_time"
  ,"t_merged_leads":"create_time"
  ,"t_ccc_feedback":"create_time"
  ,"t_leads_minisite":"create_time"
  ,"t_temp_leads":"create_time"
  ,"t_ob_leads":"create_time"
  },
  "cxbdb" : {
  "t_vehicle_peccancy":"createtime"
  ,"t_invitation_code":"createtime"
  ,"t_private_message":"createtime"
  ,"t_virtual_order":"createtime"
  ,"t_activity_present_detail":"createtime"
  ,"t_activity_view":"createtime"
  ,"t_code_type":"createtime"
  ,"t_user_feedback":"createtime"
  ,"t_generalize_user":"createtime"
  ,"t_credit_winner":"createtime"
  ,"t_designated_driving_info":"createtime"
  ,"t_obd_info":"createtime"
  ,"t_activity_banner":"createtime"
  ,"t_activity_present":"createtime"
  ,"t_member_recruit_info":"createtime"
  },
  "eadccc" : {
  "t_send_sms_log":"create_time"
  ,"t_attachment":"create_time"
  ,"t_import_file":"createtime"
  ,"t_task_execute_record":"create_time"
  ,"t_know_faq_search":"create_time"
  ,"t_service_record_detail_ext":"create_time"
  ,"t_service_request_busilabel":"create_time"
  ,"t_it_mainten_data":"create_order_time"
  ,"t_email_template":"create_time"
  ,"t_task_worksheet_ext":"update_time"
  ,"t_it_general_reception_data_ext":"create_time"
  ,"t_email_address":"create_time"
  ,"t_it_mainten_result":"create_order_time"
  ,"t_schedule":"create_time"
  ,"t_send_sms_type":"create_time"
  ,"t_it_leads_data":"created_time"
  ,"t_it_used_car_data":"createtime"
  ,"t_subscribe_inst":"create_time"
  ,"t_task_worksheet_ext_cfg_item":"create_time"
  ,"t_it_task_activities":"create_time"
  ,"t_know_file":"create_time"
  ,"t_service_record":"create_time"
  ,"t_it_mainten_data":"create_time"
  ,"t_sale_leads_validation":"create_time"
  ,"t_it_general_receotion_data_cfg":"create_time"
  ,"t_it_order_first_data":"create_time"
  ,"t_task_worksheet_ext_cfg":"update_time"
  ,"t_it_order_second_data":"create_time"
  ,"t_service_record_detail_ext_cfg":"create_time"
  ,"t_know_file":"create_name"
  ,"t_service_record_detail":"create_time"
  ,"t_email_config":"create_time"
  ,"t_it_mainten_result":"create_time"
  ,"t_it_order_first_result":"create_time"
  ,"t_it_general_reception_data":"create_time"
  ,"t_sr_worksheet":"create_time"
  ,"t_it_order_tio_data":"create_time"
  ,"t_it_order_tio_result":"create_time"
  ,"t_call_record":"create_time"
  ,"t_task_worksheet_info":"create_time"
  ,"t_it_order_activities_data":"create_date"
  ,"t_it_activities_data":"create_date"
  ,"t_it_leads_result":"create_time"
  ,"t_it_leads_data":"create_time"
  ,"t_it_used_car_leads":"create_time"
  ,"t_it_used_car_feedback_result":"create_time"
  ,"t_it_used_car_result":"create_time"
  ,"t_import_file_detail":"create_time"
  ,"t_sale_leads":"create_time"
  ,"t_it_sale_leads":"create_time"
  ,"t_questionnaire_answer":"create_time"
  },
  "grape20" : {
  "t_user_coupon" : "createddatetime"
  ,"t_user_feedback" : "createddatetime"
  ,"t_user_order" : "createddatetime"
  ,"t_user_order_basic_maintenance":"createddatetime"
  ,"t_user_order_basic_spare_part":"createddatetime"
  ,"t_user_order_comment":"createddatetime"
  ,"t_user_order_dealer_maintenance":"createddatetime"
  ,"t_user_order_dealer_spare_part":"createddatetime"
  ,"t_user_order_history":"createddatetime"
  ,"t_user_order_suit_maintenance":"createddatetime"
  ,"t_user_order_suit_spare_part":"createddatetime"
  },
  "mdmpartner" : {
  "t_entity_history" : "create_time"
  },
  "ces_score_app" : {
  "t_draw_lottery_record":"create_time"
  ,"t_insurance_policy":"create_time"
  ,"t_order_inter":"create_time"
  ,"t_prize_consump":"create_time"
  ,"t_prize_settle":"create_time"
  ,"t_score_inter":"create_time"
  ,"t_sec_kill_record":"create_time"
  },
  "mms" : {
  "t_asset_match_log_out":"create_time"
  ,"t_customer":"create_time"
  ,"t_invite_sign":"create_time"
  ,"t_member":"create_time"
  ,"t_member_card_info":"create_time"
  ,"t_member_import":"create_time"
  ,"t_member_right":"create_time"
  ,"t_member_sms":"create_time"
  ,"t_member_task":"createtime"
  ,"t_member_task_service":"createtime"
  ,"t_pingan_insurance":"create_time"
  ,"t_user_audit_pool":"create_time"
  ,"t_user_coupon_mapping":"create_time"
  ,"t_user_record":"create_time"
  ,"t_user_right":"create_time"
  },
  "mmstask" : {},
  "sso" : {
  "t_user":"create_time"
  ,"t_user_alias":"create_time"
  },
  "pay_center" : {
  "t_cash_refund_item":"create_time"
  ,"t_payment_callback":"create_time"
  ,"t_refund_callback":"create_time"
  ,"t_request_exceptions":"create_time"
  },
  "common_oms" : {
  "t_common_order":"create_time"
  ,"t_coupon":"create_time"
  ,"t_entity_history":"create_time"
  ,"t_peccancy_order":"order_create_time"
  ,"t_peccancy_order_item":"order_item_create_time"
  ,"temp_t_coupon":"create_time"
  },
  "chexiangpai" : {}  
}

conn = mysql.connector.connect(**dbConfig)
cursor = conn.cursor()
migrationDate = '2015-03-01'

scriptsDir = "scripts/"
md5EncryptStr = "md5({0}) as {0},"
createTableSqlTplWithTimeRange = "create table bimigration.%s as select %s from %s.%s where %s >= '%s';"
createTableSqlTpl = "create table bimigration.%s as select %s from %s.%s;"
tableCreateDateCol = "created_time"




for db in hiveDbs:
  secretTables = secretConfig.get(db)

  print "secretTables: "
  print secretTables
  # print secretTables
  scriptFileName = scriptsDir + db + ".hql"
  print "start to generate file: " + scriptFileName
  scriptFile = open(scriptFileName, "w")

  queryTablesSql = "select TBLS.TBL_NAME from DBS, TBLS WHERE DBS.DB_ID = TBLS.DB_ID AND DBS.NAME = %s"
  cursor.execute(queryTablesSql, (db,))
  tableList = cursor.fetchall()
  for (tableNameBytes,) in tableList:
    table_name = tableNameBytes.decode()

    queryTableColsSql = "select COLUMNS_V2.COLUMN_NAME FROM DBS, TBLS, SDS, COLUMNS_V2 WHERE DBS.DB_ID = TBLS.DB_ID AND TBLS.SD_ID = SDS.SD_ID AND SDS.CD_ID = COLUMNS_V2.CD_ID AND DBS.NAME = %s and TBLS.TBL_NAME= %s order by COLUMNS_V2.INTEGER_IDX "
    cursor.execute(queryTableColsSql, (db, table_name))
    tableColList = cursor.fetchall()
    colNames = ""
    for (tableColNameBytes,) in tableColList:
      tableColName = tableColNameBytes.decode()
      # print tableColName
      if secretTables is None:
        colNames += tableColName + ","
      else:
        secretTableCols = secretTables.get(table_name)
        if secretTableCols is None:
          colNames += tableColName + ","
        elif tableColName in secretTableCols:
          colNames += md5EncryptStr.format(tableColName)
        else :
          colNames += tableColName + ","

    hasCreateTimeCol = False
    try:
  	  tableCreateDateCol = tableCreatedDateConfig[db][table_name]
  	  hasCreateTimeCol = True
    except:
	  print "use default created_time"

    createTableSql = ""
    if hasCreateTimeCol:
      createTableSql = createTableSqlTplWithTimeRange % (table_name, colNames[0:-1], db, table_name, tableCreateDateCol, migrationDate)
    else:
      createTableSql = createTableSqlTpl % (table_name, colNames[0:-1], db, table_name)
    print createTableSql
    scriptFile.write(createTableSql + "\n")
  scriptFile.close()
  print "end to generate file: " + scriptFileName

cursor.close()
conn.close()
