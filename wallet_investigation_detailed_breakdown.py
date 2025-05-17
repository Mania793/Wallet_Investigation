import pandas as pd

def investigate_wallets(file_path):
    """
    Investigate wallet addresses to verify all five criteria:
    
    1. Three USDT transactions in January 2025.
    2. 800 USDT transaction on 3rd January.
    3. $25 transaction before the 800 USDT event.
    4. $500 - $700 transaction on 10th January.
    5. Sent to more than one address.
    
    Parameters:
    file_path (str): Path to the Parquet file containing transaction data.

    Returns:
    DataFrame: A detailed breakdown of each wallet that matches the criteria.
    """
    
    # Load the data into a DataFrame
    data = pd.read_parquet(file_path)
    
    # Convert the Unix timestamp to a readable datetime format for easier filtering
    data['datetime'] = pd.to_datetime(data['unixtimestamp'], unit='s')

    # Step 1: Filter transactions for USDT in January 2025
    jan_2025_transactions = data[
        (data['token'] == 'USDT') & 
        (data['datetime'].dt.year == 2025) & 
        (data['datetime'].dt.month == 1)
    ]

    # Step 2: Find the 800 USDT transaction on 3rd January
    transaction_800 = jan_2025_transactions[
        (jan_2025_transactions['usd_value'] >= 799) & 
        (jan_2025_transactions['usd_value'] <= 815) & 
        (jan_2025_transactions['datetime'].dt.day == 3)
    ]

    # Step 3: Identify the $25 transaction before the 800 USDT event
    if not transaction_800.empty:
        before_3rd_jan = jan_2025_transactions[
            (jan_2025_transactions['usd_value'] >= 24) & 
            (jan_2025_transactions['usd_value'] <= 26) & 
            (jan_2025_transactions['datetime'] < transaction_800['datetime'].min())
        ]
    else:
        before_3rd_jan = pd.DataFrame()

    # Step 4: Locate the $500 - $700 transaction on 10th January
    transaction_500_700 = jan_2025_transactions[
        (jan_2025_transactions['usd_value'] >= 500) & 
        (jan_2025_transactions['usd_value'] <= 700) & 
        (jan_2025_transactions['datetime'].dt.day == 10)
    ]

    # Step 5: Find common addresses that executed all three types of transactions
    from_addresses = set(transaction_800['fromAddress']).intersection(
        set(before_3rd_jan['fromAddress'])
    ).intersection(
        set(transaction_500_700['fromAddress'])
    )

    # Step 6: Ensure they sent USDT to more than one unique address
    address_summary = jan_2025_transactions[jan_2025_transactions['fromAddress'].isin(from_addresses)]
    multiple_to_addresses = address_summary.groupby('fromAddress')['toAddress'].nunique()
    valid_addresses = multiple_to_addresses[multiple_to_addresses > 1].index
    final_wallets = set(valid_addresses).intersection(from_addresses)

    # Prepare detailed breakdown
    wallet_details = []

    for wallet in final_wallets:
        details = {
            'wallet': wallet,
            '800_USDT': transaction_800[transaction_800['fromAddress'] == wallet].to_dict(orient='records'),
            '25_USDT_before_800': before_3rd_jan[before_3rd_jan['fromAddress'] == wallet].to_dict(orient='records'),
            '500_to_700_USDT': transaction_500_700[transaction_500_700['fromAddress'] == wallet].to_dict(orient='records'),
            'multiple_addresses': len(address_summary[address_summary['fromAddress'] == wallet]['toAddress'].unique())
        }
        wallet_details.append(details)

    # Convert to DataFrame for easy viewing
    return pd.DataFrame(wallet_details)

# Usage example:
breakdown_df = investigate_wallets(r'C:\Users\cuten\PycharmProjects\pythonProject\Wallet-Investigation\tron_blockchain_interview_data.parquet')
print(breakdown_df)
