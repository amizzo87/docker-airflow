"""Example DAG demonstrating the usage of the PythonOperator."""

import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

args = { 'owner': 'Marc Carvalho',
"start_date": datetime(2019, 3, 5),}

dag = DAG(
    dag_id='trr_tickets',
    default_args=args,
    schedule_interval = None,
    #schedule_interval='*/5 * * * *',
    tags=['payment api test'])


# [START howto_operator_python]

#!/usr/bin/python3
def trr_tickets():

    import re
    import boto3
    from airflow.contrib.hooks.aws_hook import AwsHook
    from botocore.exceptions import ClientError
    import pandas as pd
    import requests
    import json
    import urllib3
    urllib3.disable_warnings()

    def get_stripe():
        secret_name = "stripe_2" #secret name

        session = AwsHook(aws_conn_id='aws_default')
        client = session.get_client_type('secretsmanager')


        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print("The requested secret " + secret_name + " was not found")
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                print("The request was invalid due to:", e)
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                print("The request had invalid params:", e)
        else:
            # Decrypted secret using the associated KMS CMK
            # Depending on whether the secret was a string or binary, one of these fields will be populated
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            else:
                binary_secret_data = get_secret_value_response['SecretBinary']
                
            j = json.loads(get_secret_value_response[u'SecretString'])
            stripe_creds = dict(j)['stripe_secret'] #secret key

            return stripe_creds



    def get_zendesk():
        secret_name = "zendesk" #secret name

        session = AwsHook(aws_conn_id='aws_default')
        client = session.get_client_type('secretsmanager')

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print("The requested secret " + secret_name + " was not found")
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                print("The request was invalid due to:", e)
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                print("The request had invalid params:", e)
        else:
            # Decrypted secret using the associated KMS CMK
            # Depending on whether the secret was a string or binary, one of these fields will be populated
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            else:
                binary_secret_data = get_secret_value_response['SecretBinary']
                
            return get_secret_value_response

    j = json.loads(get_zendesk()[u'SecretString'])
    creds = dict(json.loads(j['zendesk_secret'])) #secret key

    from zenpy import Zenpy
    zenpy_client = Zenpy(**creds)
    from zenpy.lib.api_objects import CustomField, Ticket, Comment, User, Macro, MacroResult, SlaPolicy, Group
    import stripe
    stripe.api_key = get_stripe()
    from datetime import datetime, timedelta
    import delorean


    ##### Fuction to create zendesk ticket ####

    def trr_create_ticket(pay_id,buyer_id,item_id,seller_id,charge_id,item_title,card_name,card_brand,card_zip_check,last_4,card_zip,billing_name,
                            billing_zip_code,ship_name,ship_line_1,ship_city, ship_state, ship_zip):

        zp_ticket = zenpy_client.tickets.create(Ticket(subject='TRR SAO INVESTIGATION: PAYMENT_ID '+"""{PAY_ID}""".format(PAY_ID=pay_id),
                                                description=trr_macro(pay_id,item_id,buyer_id,seller_id,charge_id,item_title,card_name,card_brand,card_zip_check,last_4,card_zip,billing_name,
                                                                        billing_zip_code,ship_name,ship_line_1,ship_city, ship_state, ship_zip),tags=['trr_ticket'],
                                                    custom_fields=[CustomField(id=23918907, value="""{BUYER_ID}""".format(BUYER_ID=seller_id)),
                                                                CustomField(id=360000141223, value="""{PAY_ID}""".format(PAY_ID=pay_id)),

                                                                CustomField(id=24266176, value="payment_task"),
                                                                CustomField(id=25316683, value="disposition_tns_risk_trr"),]))
        
        
        print (zp_ticket.ticket.id)

        
    ##### Fuction to create the macro for zendesk ticket ####
        
    def trr_macro(pay_id,item_id,buyer_id,seller_id,charge_id,item_title,card_name,card_brand,card_zip_check,last_4,card_zip,billing_name,
                billing_zip_code,ship_name,ship_line_1,ship_city, ship_state, ship_zip):
        msg = """

    1. Hyperlinks:

        Payment link - https://ouadm.offerupnow.com/ouadm/payments/payment/?id={PAY_ID}

        Item link - https://ouadm.offerupnow.com/ouadm/item/allitem/{ITEM_ID}

        Buyer link: https://ouadm.offerupnow.com/ouadm/ouaccount/userprofile/?user__id={BUYER_ID}

        Seller link: https://ouadm.offerupnow.com/ouadm/ouaccount/userprofile/?user__id={SELLER_ID}

        Seller History link: https://ouadm.offerupnow.com/ouadm/payments/paymentuserdata/?user__id={SELLER_ID}

        Stripe charge: https://dashboard.stripe.com/payments/{CHARGE_ID}



    2. Charge Data INFO:

        Payment_ID: {PAY_ID}

        Item title: {ITEM_TITLE}

        Card Name: {CARD_NAME}

        Card Brand: {CARD_BRAND}

        Card Last 4: {LAST_4}

        Card Zipcode: {CARD_ZIP}    

        Zipcode Card Check : {CARD_ZIP_CHECK}



    3. Billing INFO:

        Billing Name: {BILLING_NAME}

        Billing Zip Code: {BILLING_ZIP_CODE}



    4. Shipping INFO:

        Shipping Name: {SHIP_NAME}

        Shpping Address: {SHIP_LINE_1}

        Shipping City: {SHIP_CITY}

        Shipping State: {SHIP_STATE}

        Shipping Zip Code: {SHIP_ZIP}



    Trust & Safety Team""".format(PAY_ID=pay_id, ITEM_ID=item_id, BUYER_ID=buyer_id, SELLER_ID=seller_id, CHARGE_ID=charge_id, ITEM_TITLE=item_title, CARD_NAME=card_name, 
                                CARD_ZIP_CHECK=card_zip_check, LAST_4=last_4, CARD_ZIP=card_zip, CARD_BRAND=card_brand, BILLING_NAME=billing_name, BILLING_ZIP_CODE=billing_zip_code,
                                SHIP_NAME=ship_name,SHIP_LINE_1=ship_line_1, SHIP_CITY=ship_city, SHIP_STATE=ship_state, SHIP_ZIP=ship_zip)

        return msg


    ##### Fuction to get the lastest status of a payment ####
    def get_pay_id_info(df_f):
        id_lst = []
        status_lst = []
        review_status_lst = []
        review_notes_lst = []
        marked_for_review_by_lst = []
        marked_as_resolved_by_lst = []

        for i in range(len(df_f)):
            c = df_f['PAY_ID'][i]
            try:
            

                response = requests.get('https://ouadm-internal.prod.offerup.services/internal/payments/v1/payments/'+str(c)+'', verify=False)
                values = json.loads(response.content)
                #TEMPORARY
                # print("iteration " + str(i))
                # print("Payment ID " + str(c))
                # print("SUCCESS")
                # print(response.content)
                #TEMPORARY

                for i in values['data']['payment'].items():
                    if i[0] == 'id':
                        try:
                            id_lst.append(i[1]) 
                        except:
                            id_lst.append(None)
                    if i[0] == 'payment_status':
                        try:
                            status_lst.append(i[1])
                        except:
                            status_lst.append(None)
                    if i[0] == 'review_status':
                        try:
                            review_status_lst.append(i[1])
                        except:
                            review_status_lst.append(None)                

                    if i[0] == 'review_notes':
                        try:
                            review_notes_lst.append(i[1])
                        except:
                            review_notes_lst.append(None)                


                    if i[0] == 'marked_for_review_by':
                        try:
                            marked_for_review_by_lst.append(i[1])
                        except:
                            marked_for_review_by_lst.append(None)   

                    if i[0] == 'marked_as_resolved_by':
                        try:
                            marked_as_resolved_by_lst.append(i[1])
                        except:
                            marked_as_resolved_by_lst.append(None)
                            
            except Exception as e:
                id_lst.append(None) 
                status_lst.append(None) 
                review_status_lst.append(None) 
                review_notes_lst.append(None) 
                marked_for_review_by_lst.append(None) 
                marked_as_resolved_by_lst.append(None) 
                print ('FAIL TO GET PAYMENT INFORMATION FROM INTERNAL PROD')
                print (e)


        df_f['PAYMENT_ID'] = id_lst
        df_f['PAYMENT STATUS'] = status_lst
        df_f['REVIEW_STATUS'] = review_status_lst
        df_f['REVIEW_NOTES'] = review_notes_lst
        df_f['MARKED_FOR_REVIEW'] = marked_for_review_by_lst
        df_f['MARKED_FOR_RESOLVED'] = marked_as_resolved_by_lst
        
        print (len(df_f))

        return df_f


    ##### Fuction to get the lastest payments marked for review ####
    def get_trr_charges():
        
        dt = datetime.utcnow()
        lte = delorean.Delorean(dt, timezone="UTC").epoch
        a =  datetime.now() + timedelta(hours=-1)
        gte = delorean.Delorean(a, timezone="UTC").epoch
        

        cat_lst = []
        rule_lst = []
        source_lst = []    
        charge_lst = []
        pay_id_lst = []
        item_id_lst = []
        buyer_id_lst = []
        seller_id_lst = []
        seller_age_lst = []
        buyer_age_lst = []
        item_title_lst = []
        billing_zip_lst = []
        billing_name_lst = []
        ship_name_lst = []
        ship_line_1_lst = []
        ship_city_lst = []
        ship_state_lst = []
        ship_zip_lst = []
        card_name_lst = []
        card_brand_lst = []
        card_lst_4_lst = []
        card_zip_lst = []
        card_zip_check_lst = []


        for i in stripe.Review.list(created={'gte':int(gte),'lte':int(lte)}).auto_paging_iter():
            charges = stripe.Charge.retrieve(i.charge)
            if charges.metadata.ou_source_service == 'SellerAds':
                pass
            else:
                try:
                    cat_lst.append(charges.metadata.ou_item_category)
                except:
                    cat_lst.append('nan')

                try:
                    rule_lst.append(charges.outcome.rule)
                except:
                    rule_lst.append('nan')
                    
                try:
                    source_lst.append(charges.payment_method_details.card.wallet.type)
                except:
                    source_lst.append('none')
                    
                try:
                    charge_lst.append(charges.id)
                except:
                    charge_lst.append('nan')
                try:
                    pay_id_lst.append(charges.metadata.ou_payment_id)
                except:
                    pay_id_lst.append('nan')
                try:
                    item_id_lst.append(charges.metadata.ou_item_id)
                except:
                    item_id_lst.append('nan')
                try:
                    item_title_lst.append(charges.metadata.ou_item_title)
                except:
                    item_title_lst.append('nan')    
                try:
                    buyer_id_lst.append(charges.metadata.ou_buyer_id)
                except:
                    buyer_id_lst.append('nan')      
                try:
                    buyer_age_lst.append(charges.metadata.ou_buyer_acct_age_in_hours)
                except:
                    buyer_age_lst.append('nan') 
                try:
                    seller_id_lst.append(charges.metadata.ou_seller_id)
                except:
                    seller_id_lst.append('nan')     
                try:
                    seller_age_lst.append(charges.metadata.ou_seller_acct_age_in_hours)
                except:
                    seller_age_lst.append('nan')
                try:
                    billing_zip_lst.append(charges.billing_details.address.postal_code)
                except:
                    billing_zip_lst.append('nan')    
                try:
                    billing_name_lst.append(charges.billing_details.name)
                except:
                    billing_name_lst.append('nan')    

                ##### SHIPING INFO
                try:
                    ship_name_lst.append(charges.shipping.name)
                except:
                    ship_name_lst.append('nan')     
                try:
                    ship_line_1_lst.append(charges.shipping.address.line1)
                except:
                    ship_line_1_lst.append('nan') 
                try:
                    ship_city_lst.append(charges.shipping.address.city)
                except:
                    ship_city_lst.append('nan') 
                try:
                    ship_state_lst.append(charges.shipping.address.state)
                except:
                    ship_state_lst.append('nan')
                try:
                    ship_zip_lst.append(charges.shipping.address.postal_code)
                except:
                    ship_zip_lst.append('nan') 

                ###### CARD INFO
                try:
                    card_name_lst.append(charges.source.name)
                except:
                    card_name_lst.append('nan')
                try:
                    card_brand_lst.append(charges.source.brand)
                except:
                    card_brand_lst.append('nan')
                try:
                    card_lst_4_lst.append(charges.source.last4)
                except:
                    card_lst_4_lst.append('nan')
                try:
                    card_zip_lst.append(charges.source.address_zip)
                except:
                    card_zip_lst.append('nan')
                try:
                    card_zip_check_lst.append(charges.source.address_zip_check)
                except:
                    card_zip_check_lst.append('nan')     



        df_f = pd.DataFrame(pay_id_lst,columns=['PAY_ID'])
        df_f['ITEM_ID'] = item_id_lst
        df_f['SELLER_ID'] = seller_id_lst
        df_f['SELLER_ACC_AGE'] = seller_age_lst
        df_f['BUYER_ID'] = buyer_id_lst
        df_f['BUYER_ACC_AGE'] = buyer_age_lst
        df_f['ITEM_TITLE'] = item_title_lst
        df_f['ITEM_CATEGORY'] = cat_lst
        df_f['RADAR_RULE'] = rule_lst
        df_f['PAYMENT_SOURCE'] = source_lst  
        df_f['STRIPE_CHARGE_ID'] = charge_lst
        df_f['BILLING_ZIP_CODE'] = billing_zip_lst
        df_f['BILLING_NAME'] = billing_name_lst
        df_f['SHIP_NAME'] = ship_name_lst
        df_f['SHIP_LINE_1'] = ship_line_1_lst
        df_f['SHIP_CITY'] = ship_city_lst
        df_f['SHIP_STATE'] = ship_state_lst
        df_f['SHIP_ZIP'] = ship_zip_lst
        df_f['CARD_NAME'] = card_name_lst
        df_f['CARD_BRAND'] = card_brand_lst
        df_f['CARD_LAST_4'] = card_lst_4_lst
        df_f['CARD_ZIP'] = card_zip_lst
        df_f['CARD_ZIP_CHECK'] = card_zip_check_lst
        

        return df_f

    ##### Fuction return a list of payment id for zendesk tickets created past 24hr. This fuction is used to avoid created duplicated tickets ####
    def get_zendesk_tickets():
        pay_id_lst = []

        for ticket in zenpy_client.search("tags:trr_ticket updated>24hour"):
            for c in ticket.custom_fields:
                try:
                    if c['id'] == 360000141223: 
                        pay_id_lst.append(str(c['value']))
                except:
                    pay_id_lst.append('nan')


        return list(pay_id_lst) 


    def created_zendesk_ticket(df_f, zendesk_ticket_id_lst):
        
        for i in range(len(df_f)):
        
            try:

                if (df_f['PAY_ID'][i] not in zendesk_ticket_id_lst):
                    
    ##### The below code will generate tickets into Zendesk.
                    
                    
    #                 trr_create_ticket(row['PAY_ID'],row['BUYER_ID'],row['ITEM_ID'],row['SELLER_ID'],row['STRIPE_CHARGE_ID'],row['ITEM_TITLE'],row['CARD_NAME'],row['CARD_BRAND'],
    #                               row['CARD_ZIP_CHECK'],row['CARD_LAST_4'],row['CARD_ZIP'], row['BILLING_NAME'], row['BILLING_ZIP_CODE'], row['SHIP_NAME'], row['SHIP_LINE_1'], row['SHIP_CITY'], row['SHIP_STATE']
    #                              , row['SHIP_ZIP'])




                    print (df_f['PAY_ID'][i])
                    print ('')


            except Exception as e:
                print('FAIL TO CREATED A TICKET FOR PAYMENT ID = '+str(row['PAY_ID']))
                print (e)
                pass
 
    df_f = get_trr_charges()

    df_f = get_pay_id_info(df_f)

    zendesk_ticket_id_lst = get_zendesk_tickets()
    created_zendesk_ticket(df_f, zendesk_ticket_id_lst)               


run_this = PythonVirtualenvOperator(
    task_id='get_payment',
    python_callable=trr_tickets,
        requirements=[
        "requests>=2.23.0",
        "boto3>=1.12.14",
        "pandas>=1.0.1",
        "urllib3>=1.25.8",
        "zenpy>=2.0.19",
        "stripe>=2.43.0",
        "delorean>=1.0.0"],
    system_site_packages=True,
    dag=dag,
)
# [END howto_operator_python]



