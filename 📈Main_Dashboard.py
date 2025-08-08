import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.graph_objects as go
import plotly.express as px

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Squid Bridge Metrics",
    page_icon="https://img.cryptorank.io/coins/squid1675241862798.png",
    layout="wide"
)

# --- Title with Logo -----------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="display: flex; align-items: center; gap: 15px;">
        <img src="https://img.cryptorank.io/coins/squid1675241862798.png" alt="Squid Logo" style="width:60px; height:60px;">
        <h1 style="margin: 0;">Squid Bridge Metrics</h1>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Builder Info ---------------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="margin-top: 20px; margin-bottom: 20px; font-size: 16px;">
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" style="width:25px; height:25px; border-radius: 50%;">
            <span>Built by: <a href="https://x.com/0xeman_raz" target="_blank">Eman Raz</a></span>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Snowflake Connection ----------------------------------------------------------------------------------------
conn = snowflake.connector.connect(
    user=st.secrets["snowflake"]["user"],
    password=st.secrets["snowflake"]["password"],
    account=st.secrets["snowflake"]["account"],
    warehouse="SNOWFLAKE_LEARNING_WH",
    database="AXELAR",
    schema="PUBLIC"
)

# --- Date Inputs ---------------------------------------------------------------------------------------------------
timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])
start_date = st.date_input("Start Date", value=pd.to_datetime("2023-01-01"))
end_date = st.date_input("End Date", value=pd.to_datetime("2025-07-31"))
# --- Query Function: Row1 --------------------------------------------------------------------------------------
@st.cache_data
def load_kpi_data(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        -- Token Transfers
        SELECT 
            created_at, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            recipient_address AS user, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) THEN NULL
              WHEN IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
              WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd,
            CASE 
              WHEN IS_ARRAY(data:send:fee_value) THEN NULL
              WHEN IS_OBJECT(data:send:fee_value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
              ELSE NULL
            END AS fee,
            id, 
            'Token Transfers' AS Service, 
            data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )

        UNION ALL

        -- GMP
        SELECT  
            created_at,
            data:call.chain::STRING AS source_chain,
            data:call.returnValues.destinationChain::STRING AS destination_chain,
            data:call.transaction.from::STRING AS user,
            CASE 
              WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
              ELSE NULL
            END AS amount_usd,
            COALESCE(
              CASE 
                WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                  OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
                THEN NULL
                WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                  AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                ELSE NULL
              END,
              CASE 
                WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                ELSE NULL
              END
            ) AS fee,
            id, 
            'GMP' AS Service, 
            data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
    )
    SELECT 
        COUNT(DISTINCT id) AS Number_of_Transfers, 
        COUNT(DISTINCT user) AS Number_of_Users, 
        ROUND(SUM(amount_usd)) AS Volume_of_Transfers
    FROM axelar_service
    WHERE created_at::date >= '{start_str}' 
      AND created_at::date <= '{end_str}'
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data ----------------------------------------------------------------------------------------------------
df_kpi = load_kpi_data(timeframe, start_date, end_date)

# --- KPI Row ------------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

col1.metric(
    label="Volume of Transfers",
    value=f"${df_kpi['VOLUME_OF_TRANSFERS'][0]:,}"
)

col2.metric(
    label="Number of Transfers",
    value=f"{df_kpi['NUMBER_OF_TRANSFERS'][0]:,} Txns"
)

col3.metric(
    label="Number of Users",
    value=f"{df_kpi['NUMBER_OF_USERS'][0]:,} Addresses"
)

# --- Query Function: Row (2) --------------------------------------------------------------------------------------
@st.cache_data
def load_time_series_data(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        -- Token Transfers
        SELECT 
            created_at, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            recipient_address AS user, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) THEN NULL
              WHEN IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
              WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd,
            CASE 
              WHEN IS_ARRAY(data:send:fee_value) THEN NULL
              WHEN IS_OBJECT(data:send:fee_value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
              ELSE NULL
            END AS fee,
            id, 
            'Token Transfers' AS Service, 
            data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )

        UNION ALL

        -- GMP
        SELECT  
            created_at,
            data:call.chain::STRING AS source_chain,
            data:call.returnValues.destinationChain::STRING AS destination_chain,
            data:call.transaction.from::STRING AS user,
            CASE 
              WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
              ELSE NULL
            END AS amount_usd,
            COALESCE(
              CASE 
                WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                  OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
                THEN NULL
                WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                  AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                ELSE NULL
              END,
              CASE 
                WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                ELSE NULL
              END
            ) AS fee,
            id, 
            'GMP' AS Service, 
            data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
    )
    SELECT 
        DATE_TRUNC('{timeframe}', created_at) AS Date,
        COUNT(DISTINCT id) AS Number_of_Transfers, 
        COUNT(DISTINCT user) AS Number_of_Users, 
        ROUND(SUM(amount_usd)) AS Volume_of_Transfers
    FROM axelar_service
    WHERE created_at::date >= '{start_str}' 
      AND created_at::date <= '{end_str}'
    GROUP BY 1
    ORDER BY 1
    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
df_ts = load_time_series_data(timeframe, start_date, end_date)

# --- Charts in One Row ---------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    fig1 = px.bar(df_ts, x="DATE", y="VOLUME_OF_TRANSFERS",
                  title="Squid Bridge Volume Over Time (USD)",
                  labels={"VOLUME_OF_TRANSFERS": "Volume (USD)", "DATE": "Date"})
    fig1.update_layout(xaxis_title="", yaxis_title="USD", bargap=0.2)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.bar(df_ts, x="DATE", y="NUMBER_OF_TRANSFERS",
                  title="Squid Bridge Transactions Over Time",
                  labels={"NUMBER_OF_TRANSFERS": "Transactions", "DATE": "Date"})
    fig2.update_layout(xaxis_title="", yaxis_title="Txns", bargap=0.2)
    st.plotly_chart(fig2, use_container_width=True)

with col3:
    fig3 = px.bar(df_ts, x="DATE", y="NUMBER_OF_USERS",
                  title="Squid Bridge Users Over Time",
                  labels={"NUMBER_OF_USERS": "Users", "DATE": "Date"})
    fig3.update_layout(xaxis_title="", yaxis_title="Addresses", bargap=0.2)
    st.plotly_chart(fig3, use_container_width=True)

# ----------------------------------------------------------------------------------------------------------------------------
# --- Query Function: Row (3) ------------------------------------------------------------------------------------------------
@st.cache_data
def load_source_chain_data(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        -- Token Transfers
        SELECT 
            created_at, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            recipient_address AS user, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) THEN NULL
              WHEN IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
              WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd,
            CASE 
              WHEN IS_ARRAY(data:send:fee_value) THEN NULL
              WHEN IS_OBJECT(data:send:fee_value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
              ELSE NULL
            END AS fee,
            id, 
            'Token Transfers' AS Service, 
            data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND created_at::date >= '{start_str}' 
          AND created_at::date <= '{end_str}'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )

        UNION ALL

        -- GMP
        SELECT  
            created_at,
            data:call.chain::STRING AS source_chain,
            data:call.returnValues.destinationChain::STRING AS destination_chain,
            data:call.transaction.from::STRING AS user,
            CASE 
              WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
              ELSE NULL
            END AS amount_usd,
            COALESCE(
              CASE 
                WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                  OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
                THEN NULL
                WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                  AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                ELSE NULL
              END,
              CASE 
                WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                ELSE NULL
              END
            ) AS fee,
            id, 
            'GMP' AS Service, 
            data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND created_at::date >= '{start_str}' 
          AND created_at::date <= '{end_str}'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
    )
    SELECT source_chain AS "Source Chain", 
           COUNT(DISTINCT id) AS "Number of Transfers", 
           COUNT(DISTINCT user) AS "Number of Users", 
           ROUND(SUM(amount_usd)) AS "Volume of Transfers (USD)"
    FROM axelar_service
    GROUP BY 1
    ORDER BY 4 DESC
    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
df_source = load_source_chain_data(start_date, end_date)

# --- Display Table ------------------------------------------------------------------------------------------------
st.subheader("Squid Activity by Source Chain")
st.dataframe(df_source, use_container_width=True)

# --- Top 10 Horizontal Bar Charts ----------------------------------------------------------------------------------
top_vol = df_source.nlargest(10, "Volume of Transfers (USD)")
top_txn = df_source.nlargest(10, "Number of Transfers")
top_usr = df_source.nlargest(10, "Number of Users")

col1, col2, col3 = st.columns(3)

with col1:
    fig1 = px.bar(top_vol.sort_values("Volume of Transfers (USD)"),
                  x="Volume of Transfers (USD)", y="Source Chain",
                  orientation="h",
                  title="Top 10 Source Chains by Volume (USD)")
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.bar(top_txn.sort_values("Number of Transfers"),
                  x="Number of Transfers", y="Source Chain",
                  orientation="h",
                  title="Top 10 Source Chains by Transfers")
    st.plotly_chart(fig2, use_container_width=True)

with col3:
    fig3 = px.bar(top_usr.sort_values("Number of Users"),
                  x="Number of Users", y="Source Chain",
                  orientation="h",
                  title="Top 10 Source Chains by Users")
    st.plotly_chart(fig3, use_container_width=True)

# --- Destination Chain Data Query: Row 5, 6 --------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_destination_data(start_date, end_date):
    # ensure string format YYYY-MM-DD
    start_str = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end_str = pd.to_datetime(end_date).strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
      SELECT 
        created_at, 
        LOWER(data:send:original_source_chain) AS source_chain, 
        LOWER(data:send:original_destination_chain) AS destination_chain,
        recipient_address AS user, 
        CASE 
          WHEN IS_ARRAY(data:send:amount) THEN NULL
          WHEN IS_OBJECT(data:send:amount) THEN NULL
          WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
          ELSE NULL
        END AS amount,
        CASE 
          WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
          WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
          WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
          ELSE NULL
        END AS amount_usd,
        CASE 
          WHEN IS_ARRAY(data:send:fee_value) THEN NULL
          WHEN IS_OBJECT(data:send:fee_value) THEN NULL
          WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
          ELSE NULL
        END AS fee,
        id, 
        'Token Transfers' AS "Service", 
        data:link:asset::STRING AS raw_asset
      FROM axelar.axelscan.fact_transfers
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND created_at::date >= '{start_str}'
        AND created_at::date <= '{end_str}'
        AND (
          sender_address ILIKE '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
          OR sender_address ILIKE '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
          OR sender_address ILIKE '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
          OR sender_address ILIKE '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
          OR sender_address ILIKE '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
        )

      UNION ALL

      SELECT  
        created_at,
        data:call.chain::STRING AS source_chain,
        data:call.returnValues.destinationChain::STRING AS destination_chain,
        data:call.transaction.from::STRING AS user,
        CASE 
          WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
          WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
          ELSE NULL
        END AS amount,
        CASE 
          WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
          WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
          ELSE NULL
        END AS amount_usd,
        COALESCE(
          CASE 
            WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
              OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
            THEN NULL
            WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
              AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
            ELSE NULL
          END,
          CASE 
            WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
            WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
            ELSE NULL
          END
        ) AS fee,
        id, 
        'GMP' AS "Service", 
        data:symbol::STRING AS raw_asset
      FROM axelar.axelscan.fact_gmp 
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND created_at::date >= '{start_str}'
        AND created_at::date <= '{end_str}'
        AND (
          data:approved:returnValues:contractAddress ILIKE '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
          OR data:approved:returnValues:contractAddress ILIKE '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
          OR data:approved:returnValues:contractAddress ILIKE '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
          OR data:approved:returnValues:contractAddress ILIKE '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
          OR data:approved:returnValues:contractAddress ILIKE '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
        )
    )

    SELECT 
      destination_chain AS "Destination Chain", 
      COUNT(DISTINCT id) AS "Number of Transfers", 
      COUNT(DISTINCT user) AS "Number of Users", 
      ROUND(SUM(amount_usd)) AS "Volume of Transfers (USD)"
    FROM axelar_service
    GROUP BY 1
    ORDER BY "Volume of Transfers (USD)" DESC
    """

    df = pd.read_sql(query, conn)

    # normalize column names for easier downstream use
    df = df.rename(columns={
        "Destination Chain": "destination_chain",
        "Number of Transfers": "number_of_transfers",
        "Number of Users": "number_of_users",
        "Volume of Transfers (USD)": "volume_usd"
    })

    return df

# --- Use the cached loader ---------------------------------------------------------
df_dest = load_destination_data(start_date, end_date)

# --- show table ---
st.subheader("Destination Chain Metrics")
st.dataframe(df_dest, use_container_width=True)

# --- prepare top-10s and charts (horizontal bars) ------------------------------------
top_vol_dest = df_dest.nlargest(10, "volume_usd").sort_values("volume_usd", ascending=False)
top_txn_dest = df_dest.nlargest(10, "number_of_transfers").sort_values("number_of_transfers", ascending=False)
top_usr_dest = df_dest.nlargest(10, "number_of_users").sort_values("number_of_users", ascending=False)

fig_vol_dest = px.bar(
    top_vol_dest,
    x="volume_usd",
    y="destination_chain",
    orientation="h",
    title="Top 10 Destination Chains by Volume (USD)",
    labels={"volume_usd": "Volume (USD)", "destination_chain": "Destination Chain"}
)
fig_vol_dest.update_xaxes(tickformat=",.0f")
fig_vol_dest.update_traces(hovertemplate="%{y}: $%{x:,.0f}<extra></extra>")

fig_txn_dest = px.bar(
    top_txn_dest,
    x="number_of_transfers",
    y="destination_chain",
    orientation="h",
    title="Top 10 Destination Chains by Transfers",
    labels={"number_of_transfers": "Txns", "destination_chain": "Destination Chain"}
)
fig_txn_dest.update_xaxes(tickformat=",.0f")
fig_txn_dest.update_traces(hovertemplate="%{y}: %{x:,}<extra></extra>")

fig_usr_dest = px.bar(
    top_usr_dest,
    x="number_of_users",
    y="destination_chain",
    orientation="h",
    title="Top 10 Destination Chains by Users",
    labels={"number_of_users": "Addresses", "destination_chain": "Destination Chain"}
)
fig_usr_dest.update_xaxes(tickformat=",.0f")
fig_usr_dest.update_traces(hovertemplate="%{y}: %{x:,}<extra></extra>")

# --- display three charts in one row -----------------------------------------------
col1, col2, col3 = st.columns(3)
with col1:
    st.plotly_chart(fig_vol_dest, use_container_width=True)
with col2:
    st.plotly_chart(fig_txn_dest, use_container_width=True)
with col3:
    st.plotly_chart(fig_usr_dest, use_container_width=True)

# --- Cached loader for Path metrics: Row 7, 8 ----------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_path_data(start_date, end_date):
    start_str = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end_str = pd.to_datetime(end_date).strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
      SELECT 
        created_at, 
        LOWER(data:send:original_source_chain) AS source_chain, 
        LOWER(data:send:original_destination_chain) AS destination_chain,
        recipient_address AS user, 
        CASE 
          WHEN IS_ARRAY(data:send:amount) THEN NULL
          WHEN IS_OBJECT(data:send:amount) THEN NULL
          WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
          ELSE NULL
        END AS amount,
        CASE 
          WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
          WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
          WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
          ELSE NULL
        END AS amount_usd,
        CASE 
          WHEN IS_ARRAY(data:send:fee_value) THEN NULL
          WHEN IS_OBJECT(data:send:fee_value) THEN NULL
          WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
          ELSE NULL
        END AS fee,
        id, 
        'Token Transfers' AS "Service", 
        data:link:asset::STRING AS raw_asset
      FROM axelar.axelscan.fact_transfers
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND created_at::date >= '{start_str}'
        AND created_at::date <= '{end_str}'
        AND (
          sender_address ILIKE '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
          OR sender_address ILIKE '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
          OR sender_address ILIKE '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
          OR sender_address ILIKE '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
          OR sender_address ILIKE '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
        )
      UNION ALL
      SELECT  
        created_at,
        data:call.chain::STRING AS source_chain,
        data:call.returnValues.destinationChain::STRING AS destination_chain,
        data:call.transaction.from::STRING AS user,
        CASE 
          WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
          WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
          ELSE NULL
        END AS amount,
        CASE 
          WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
          WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
          ELSE NULL
        END AS amount_usd,
        COALESCE(
          CASE 
            WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
              OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
            THEN NULL
            WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
              AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
            ELSE NULL
          END,
          CASE 
            WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
            WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
            ELSE NULL
          END
        ) AS fee,
        id, 
        'GMP' AS "Service", 
        data:symbol::STRING AS raw_asset
      FROM axelar.axelscan.fact_gmp 
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND created_at::date >= '{start_str}'
        AND created_at::date <= '{end_str}'
        AND (
          data:approved:returnValues:contractAddress ILIKE '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
          OR data:approved:returnValues:contractAddress ILIKE '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
          OR data:approved:returnValues:contractAddress ILIKE '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
          OR data:approved:returnValues:contractAddress ILIKE '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
          OR data:approved:returnValues:contractAddress ILIKE '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
        )
    )
    SELECT 
      source_chain || '➡' || destination_chain AS path, 
      COUNT(DISTINCT id) AS number_of_transfers, 
      COUNT(DISTINCT user) AS number_of_users, 
      ROUND(SUM(amount_usd)) AS volume_usd
    FROM axelar_service
    GROUP BY 1
    ORDER BY number_of_transfers DESC
    """

    return pd.read_sql(query, conn)

# --- Load data ---
df_path = load_path_data(start_date, end_date)

# --- Show table ---
st.subheader("Squid Path Metrics")
st.dataframe(df_path, use_container_width=True)

# --- Query Function: Row 8 --------------------------------------------------------------------------------------
@st.cache_data
def load_new_users_data(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH overview AS (
      WITH axelar_service AS (
        SELECT created_at, recipient_address AS user
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed' AND simplified_status = 'received'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
        UNION ALL
        SELECT created_at, data:call.transaction.from::STRING AS user
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed' AND simplified_status = 'received'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
            OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
      )
      SELECT user, min(created_at::date) as first_date
      FROM axelar_service
      WHERE created_at::date >= '{start_str}' AND created_at::date <= '{end_str}'
      GROUP BY 1
    )
    SELECT 
      date_trunc('{timeframe}', first_date) as "Date", 
      count(distinct user) as "New Users",
      sum(count(distinct user)) OVER (ORDER BY date_trunc('{timeframe}', first_date)) as "Total New Users"
    FROM overview
    GROUP BY 1
    ORDER BY 1
    """

    df = pd.read_sql(query, conn)

    if df.empty:
        # اگر داده‌ای نبود، ستون‌ها رو بساز برای جلوگیری از ارور
        df = pd.DataFrame(columns=["Date", "New Users", "Total New Users"])
    else:
        df['Date'] = pd.to_datetime(df['Date'])

    return df

# --- Load Data ----------------------------------------------------------------------------------------------------
df_users = load_new_users_data(timeframe, start_date, end_date)

# --- Plot Bar-Line Chart -------------------------------------------------------------------------------------------
fig = go.Figure()

fig.add_trace(go.Bar(
    x=df_users["Date"],
    y=df_users["New Users"],
    name='New Users',
    marker_color='#e2fb43',
    yaxis='y1'
))

fig.add_trace(go.Scatter(
    x=df_users["Date"],
    y=df_users["Total New Users"],
    name='Total New Users',
    mode='lines+markers',
    line=dict(color='#ca99e5'),
    yaxis='y2'
))

fig.update_layout(
    title="New/Total Squid Users Over Time",
    xaxis=dict(title='Date'),
    yaxis=dict(
        title='New Users',
        titlefont=dict(color='#e2fb43'),
        tickfont=dict(color='#e2fb43'),
        side='left',
        showgrid=False,
    ),
    yaxis2=dict(
        title='Total New Users',
        titlefont=dict(color='#ca99e5'),
        tickfont=dict(color='#ca99e5'),
        overlaying='y',
        side='right',
        anchor='x',
        showgrid=False,
    ),
    legend=dict(x=0.01, y=0.99),
    bargap=0.2,
    template='plotly_white',
    height=500
)

st.plotly_chart(fig, use_container_width=True)


