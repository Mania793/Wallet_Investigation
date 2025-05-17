# Wallet_Investigation
This Python script investigates wallet addresses to verify if they match the specified five criteria for USDT transactions on the Tron blockchain during January 2025.

Intelligence Criteria:

Three USDT transactions in January 2025.

800 USDT transaction on 3rd January.

25 USDT transaction before the 800 USDT event.

$500 - $700 transaction on 10th January.

Sent to more than one unique address.

Requirements:

Python 3.8 or higher

pyarrow library for reading Parquet files

pandas library for data processing

Install the dependencies:

pip install pyarrow pandas

Usage:

Place your Parquet transaction file in the same directory as the script and run:

from wallet_investigation_detailed_breakdown import investigate_wallets

# Replace 'path_to_your_parquet_file.parquet' with the path to your data file
breakdown_df = investigate_wallets('path_to_your_parquet_file.parquet')
print(breakdown_df)

Output:

The script will return a detailed breakdown of each wallet that matches all five conditions:

800 USDT transaction on 3rd January.

25 USDT transaction before the 800 USDT event.

$500 - $700 transaction on 10th January.

List of unique addresses sent to.
