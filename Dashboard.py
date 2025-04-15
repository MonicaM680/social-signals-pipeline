import streamlit as st
import configparser
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px  

# ###########################################################
# # Step 1: Database Configuration and Connection
# ###########################################################

def configure_database():
    """
    Reads the database configuration from config.ini and establishes a connection.
    Returns a SQLAlchemy engine object.
    """
    try:
        config = configparser.ConfigParser()
        config.read('config.ini')
        db_config = config['database']
        host = db_config['host']
        port = db_config['port']
        user = db_config['user']
        password = db_config['password']
        transform_db = db_config['transform_db']

        escaped_password = quote_plus(password)
        connection_string = f"mysql+pymysql://{user}:{escaped_password}@{host}:{port}/{transform_db}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        st.error(f"Error configuring database: {e}")
        raise


# ###########################################################
# # Step 2: Query Functions
# ###########################################################
# Step 3: Fetch key metrics
def fetch_key_metrics():
    queries = {
        "total_orders": """
            SELECT COUNT(DISTINCT OrderID) AS total_orders
            FROM fact_order_items;
        """,
        "total_revenue": """
            SELECT SUM(PaymentValue) AS total_revenue
            FROM fact_order_items;
        """,
        "average_installments": """
            SELECT AVG(Quantity) AS average_installments
            FROM fact_order_items;
        """,
        "Delayed_Orders": """
            SELECT COUNT(DISTINCT OrderID) AS delayed_orders_count
            FROM fact_order_items
            WHERE DeliveredDateKey > EstimatedDeliveryDateKey
            AND DeliveredDateKey IS NOT NULL
            AND EstimatedDeliveryDateKey IS NOT NULL;
        """
    }
    metrics = {}
    try:
        with engine.connect() as connection:
            for metric_name, query_sql in queries.items():
                result = connection.execute(text(query_sql))
                row = result.fetchone()
                metrics[metric_name] = row[0] if row[0] is not None else 0
    except Exception as e:
        st.error(f"Error fetching key metrics: {e}")
        raise
    return metrics

# Step 4: Fetch payment distribution data
def fetch_payment_distribution():
     query = """
        SELECT 
            CASE 
                WHEN dp.PaymentType = 'Credit Card' THEN 'Credit Card'
                WHEN dp.PaymentType = 'blipay' THEN 'Blipay'
                WHEN dp.PaymentType = 'voucher' THEN 'Voucher'
                ELSE 'Others'
            END AS PaymentMethod,
            COUNT(*) AS Count
        FROM fact_order_items foi
        JOIN dim_payments dp ON foi.PaymentID = dp.PaymentID
        GROUP BY PaymentMethod;   """   
     try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            rows = result.fetchall()
            df = pd.DataFrame(rows, columns=['PaymentMethod', 'Count'])
            df['Percentage'] = df['Count'] / df['Count'].sum() * 100
            return df
     except Exception as e:
        st.error(f"Error fetching payment distribution: {e}")
        raise


# Step 5: Fetch order distribution over months
def fetch_order_distribution_months():
    query = """
        SELECT 
            CONCAT(d.Year, '-', LPAD(d.Month, 2, '0')) AS YearMonth,
            COUNT(foi.OrderID) AS OrderCount
        FROM fact_order_items foi
        JOIN date_d d ON foi.OrderDateKey = d.DateKey
        GROUP BY d.Year, d.Month
        ORDER BY d.Year, d.Month;
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            rows = result.fetchall()
            df = pd.DataFrame(rows, columns=['YearMonth','OrderCount'])
            return df
    except Exception as e:
        st.error(f"Error fetching order distribution over months: {e}")
        raise

# Step 6: Fetch peak order times by hour
def fetch_peak_order_times():
    query = """
        SELECT 
            HOUR(OrderTimeKey) AS OrderHour,
            COUNT(OrderID) AS OrderCount
        FROM fact_order_items
        GROUP BY HOUR(OrderTimeKey)
        ORDER BY OrderHour;
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            rows = result.fetchall()
            df = pd.DataFrame(rows, columns=['OrderHour', 'OrderCount'])
            return df
    except Exception as e:
        st.error(f"Error fetching peak order times: {e}")
        raise

# Step 7: Fetch total orders by season
def fetch_total_orders_by_season():
    query = """
        SELECT 
            d.Season AS Season,
            COUNT(foi.OrderID) AS TotalOrders
        FROM fact_order_items foi
        JOIN date_d d ON foi.OrderDateKey = d.DateKey
        GROUP BY d.Season
        ORDER BY 
            CASE 
                WHEN d.Season = 'Spring' THEN 1
                WHEN d.Season = 'Summer' THEN 2
                WHEN d.Season = 'Fall' THEN 3
                WHEN d.Season = 'Winter' THEN 4
                ELSE 5
            END;
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            rows = result.fetchall()
            df = pd.DataFrame(rows, columns=['Season', 'TotalOrders'])
            return df
    except Exception as e:
        st.error(f"Error fetching total orders by season: {e}")
        raise

# Step 8: Fetch average delivery delay days by feedback score
def fetch_avg_delivery_delay_by_feedback():
    query = """
        SELECT 
            df.FeedbackScore AS FeedbackScore,
            AVG(foi.DeliveryDelayDays) AS AvgDeliveryDelayDays
        FROM fact_order_items foi
        JOIN dim_feedbacks df ON foi.FeedbackID = df.FeedbackID
        WHERE df.FeedbackScore IS NOT NULL AND foi.DeliveryDelayDays IS NOT NULL
        GROUP BY df.FeedbackScore
        ORDER BY df.FeedbackScore;
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            rows = result.fetchall()
            df = pd.DataFrame(rows, columns=['FeedbackScore', 'AvgDeliveryDelayDays'])
            return df
    except Exception as e:
        st.error(f"Error fetching average delivery delay by feedback score: {e}")
        raise

# Step 9: Fetch worst traffic routes
def fetch_logistics_metrics():
    query = """
        SELECT 
    ds.SellerState AS SellerState,
    du.UserState AS UserState,
    COUNT(foi.OrderID) AS TotalOrders,
    SUM(CASE WHEN foi.DeliveryDelayDays > 0 THEN 1 ELSE 0 END) AS TotalOrdersDelayed,
    AVG(foi.DeliveryDelayDays) AS AverageDeliveryDelay,
    AVG(foi.ShippingDays) AS AvgShippingDays,
    (SUM(CASE WHEN foi.DeliveryDelayDays > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(foi.OrderID)) AS DelayPercentage
FROM 
    fact_order_items foi
JOIN 
    dim_sellers ds ON foi.SellerID = ds.SellerID
JOIN 
    dim_users du ON foi.UserID = du.UserID
GROUP BY 
    ds.SellerState, du.UserState
HAVING 
    COUNT(foi.OrderID) >= 10 -- Exclude routes with very low traffic
ORDER BY 
    TotalOrders DESC, AverageDeliveryDelay DESC;
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            rows = result.fetchall()
            df = pd.DataFrame(rows, columns=[
                'SellerState', 'UserState', 'TotalOrders', 'TotalOrdersDelayed',
                'AverageDeliveryDelay', 'AvgShippingDays','DelayPercentage'
            ])
            return df
    except Exception as e:
        st.error(f"Error fetching logistics metrics: {e}")
        raise


# Step 10: Fetch payment method preferences by product category
def fetch_sunburst_data():
    query = """
      SELECT 
    pc.ProductCategory AS ProductCategory,
    CASE 
        WHEN dp.PaymentType = 'Credit Card' THEN 'Credit Card'
        WHEN dp.PaymentType = 'blipay' THEN 'Blipay'
        WHEN dp.PaymentType = 'voucher' THEN 'Voucher'
        ELSE 'Others'
    END AS PaymentMethod,
    COUNT(foi.OrderID) AS TotalOrders
FROM fact_order_items foi
JOIN dim_products pc ON foi.ProductID = pc.ProductID
JOIN dim_payments dp ON foi.PaymentID = dp.PaymentID
WHERE pc.ProductCategory IS NOT NULL AND pc.ProductCategory != ''  -- Ignore empty or NULL categories
GROUP BY pc.ProductCategory, PaymentMethod
ORDER BY pc.ProductCategory, TotalOrders DESC;
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query))
            rows = result.fetchall()
            df = pd.DataFrame(rows, columns=['ProductCategory', 'PaymentMethod', 'TotalOrders'])
            return df
    except Exception as e:
        st.error(f"Error fetching payment method preferences by category: {e}")
        raise

#Step 11: Fetch top 20 states with the highest purchase frequency

query = """ SELECT 
    foi.UserState AS State,
    COUNT(foi.OrderID) AS PurchaseFrequency
FROM 
    fact_order_items foi
GROUP BY 
    foi.UserState
ORDER BY 
    PurchaseFrequency DESC
LIMIT 20;
"""

# ###########################################################
# # Step 12: Streamlit App Layout
# ###########################################################
st.set_page_config(layout="wide")
st.markdown(
    """
    <div style="background-color:#000000; padding:20px; border-radius:10px; text-align:center; margin-bottom:20px;">
        <h1 style="color:white; margin-bottom:0px;">📊 Social Signals: Unlocking E-commerce Trends</h1>
    </div>
    """,
    unsafe_allow_html=True
)

st.sidebar.markdown(
    """
      ## Navigating the Insights
    **Social Signals: Unlocking E-commerce Trends** dives into key metrics and actionable insights to help businesses understand customer behavior, optimize operations, and drive growth.
    
    - **[Key Metrics](#key-metrics):** Explore total orders, revenue, average installments, and delayed orders at a glance.
    - **[Payment Preferences](#payment-preferences):** Analyze the most popular payment methods and their adoption across product categories.
    - **[Order Trends](#order-trends):** Understand monthly order distributions, peak shopping hours, and seasonal patterns.
    - **[Customer Satisfaction](#customer-satisfaction):** Correlate feedback scores with delivery delays to enhance customer experience.
    - **[Logistics Analysis](#logistics-performance):** Identify high-traffic routes, delivery delays, and areas for operational improvement.
    - **[Geographic Analysis](#geographic-insights):** Discover purchase frequency by region and opportunities for market expansion.
    - **[Business Recommendations](#business-recommendations):** Get actionable strategies to optimize your business.

    This dashboard empowers businesses to make data-driven decisions, optimize strategies, and unlock new growth opportunities in the e-commerce space.
    """
)

# Initialize database connection
engine = configure_database()

# Step 13: Fetch key metrics
st.markdown('<div id="key-metrics"></div>', unsafe_allow_html=True)
metrics = fetch_key_metrics()

# Custom for KPI boxes
st.markdown(
    """
    <style>
    .kpi-container {
        display: flex;
        justify-content: space-between; /* Distribute space evenly between boxes */
        gap: 10px; /* Add spacing between boxes */
        margin-top: 20px;
    }
    .kpi-box {
        display: flex; /* Add this */
        flex-direction: column; /* Add this */
        justify-content: center; /* Centers content vertically */
        align-items: center; /* Centers content horizontally */
        padding: 15px;
        border-radius: 10px;
        text-align: center;
        flex: 1; /* Ensure all boxes take equal width */
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }

    .kpi-box h3 {
        font-size: 18px;
        margin: 0;
        color: white; /* White text for contrast */
        line-height: 1.2; /* Ensure consistent line height */
    }
    .kpi-box p {
        font-size: 24px;
        margin: 5px 0 0;
        font-weight: bold;
        color: white; /* White text for contrast */
    }
    /* Unique colors for each KPI box */
    .kpi-box.total-orders { background-color: #81C784; } /* Light Green */
    .kpi-box.total-revenue { background-color: #FFB74D; } /* Light Orange */
    .kpi-box.avg-installments { background-color: #64B5F6; } /* Light Blue */
    .kpi-box.delayed-orders { background-color: #F48FB1; } /* Light Pink */
    </style>
    """,
    unsafe_allow_html=True
)

# Display Key Metrics Section
st.markdown(
    f"""
    <div class="kpi-container">
        <div class="kpi-box total-orders">
            <h3>Total Orders</h3>
            <p>{metrics['total_orders'] / 1000:.2f}K</p>
        </div>
        <div class="kpi-box total-revenue">
            <h3>Total Revenue</h3>
            <p>${metrics['total_revenue'] / 1e9:.2f}B</p>
        </div>
        <div class="kpi-box avg-installments">
            <h3>Average Installments</h3>
            <p>{metrics['average_installments']:.2f}</p>
        </div>
        <div class="kpi-box delayed-orders">
            <h3>Delayed Orders</h3>
            <p>{metrics['Delayed_Orders']}</p>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)


# Step 14: Fetch payment distribution data
st.markdown('<div id="payment-preferences" style="padding-top: 60px; margin-top: -60px;"></div>', unsafe_allow_html=True)
payment_df = fetch_payment_distribution()
# Streamlit Header and Business Insight
st.header("Payment Preferences")
st.subheader("Most Popular Payment Methods")
# Create and Display Pie Chart
fig_payment = px.pie(
    payment_df,
    values='Percentage',
    names='PaymentMethod',
    hole=0.3,  # Donut chart style
    # title="Most Popular Payment Methods"
)
st.plotly_chart(fig_payment, use_container_width=True)

st.markdown("""
Credit cards are the most popular payment method, capturing **74.4%** of transactions, 
followed by Blipay at **19.8%**. Other methods like "Voucher" and "Others" have minimal adoption (**5.71% combined**).
""")

# Step 15: Display Sunburst Chart Visualization
sunburst_df = fetch_sunburst_data()
st.subheader("Payment Methods and Product Category Trends")
if not sunburst_df.empty:
    # Create a sunburst chart
    fig = px.sunburst(
        sunburst_df,
        path=["PaymentMethod", "ProductCategory"],  # Hierarchical structure
        values="TotalOrders",  # Size of each segment
        color="TotalOrders",  # Color segments based on order count
        color_continuous_scale="Viridis",  # Use a color gradient
        hover_data={
            "PaymentMethod": True,  # Include PaymentMethod in the tooltip
            "ProductCategory": True,  # Include ProductCategory in the tooltip
            "TotalOrders": ":,.0f"  # Format TotalOrders as an integer with commas
        }
    )
    # Customize the tooltip using hovertemplate
    fig.update_traces(
        hovertemplate=(
            "<b>Payment Method:</b> %{customdata[0]}<br>" +
            "<b>Product Category:</b> %{customdata[1]}<br>" +
            "<b>Total Orders:</b> %{value:,}<extra></extra>"
        )
    )
    # Customize layout
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=20),  # Adjust margins
    )
    
    # Render the plot in Streamlit
    st.plotly_chart(fig, use_container_width=True)

else:
    st.warning("No data available for the sunburst chart.")

 # Add key insights below the chart
st.markdown("""
    Credit Card usage peaks in categories like **Bed Bath Table**, while Blipay is concentrated in **Electronics**, and Voucher usage is highest in **Fashion Bags Accessories**. Niche categories like **Toys** and **Telephony** have lower demand across all payment methods.
    """)

# Step 16: Fetch order distribution over months
st.markdown('<div id="order-trends" style="padding-top: 60px; margin-top: -60px;"></div>', unsafe_allow_html=True)
order_distribution_months_df = fetch_order_distribution_months()

# Heading for Order Distribution Over Months
st.header("Order Trends")

# Subheading for Monthly Distribution
st.subheader("Monthly Order Distribution")
if not order_distribution_months_df.empty:

    # Create line chart for monthly order distribution
    fig_months = px.line(
        order_distribution_months_df,
        x='YearMonth',
        y='OrderCount',
        labels={'YearMonth': 'Year-Month', 'OrderCount': 'Number of Orders'},
        markers=True
    )

    # Customize the x-axis to display month names
    fig_months.update_xaxes(
        tickmode='array',  # Use custom tick positions
        tickvals=order_distribution_months_df['YearMonth'],  # Use YearMonth values as tick positions
        ticktext=order_distribution_months_df['YearMonth'],  # Use YearMonth values as tick labels
        tickangle=45,  # Rotate labels for better readability
        showgrid=False  # Remove grid lines for clarity
    )
    # Remove grid lines from y-axis
    fig_months.update_yaxes(showgrid=False)

    # Add annotation for the peak order on November 24, 2017
    peak_date = '2017-11'  # Assuming 'YearMonth' is in the format 'YYYY-MM'
    peak_order_count = order_distribution_months_df.loc[
        order_distribution_months_df['YearMonth'] == peak_date, 'OrderCount'
    ].values[0]  # Get the corresponding OrderCount for the peak date

    fig_months.add_annotation(
        x=peak_date,  # X-coordinate of the annotation (YearMonth)
        y=peak_order_count,  # Y-coordinate of the annotation (OrderCount)
        text=f"{peak_order_count} orders",  # Annotation text
        showarrow=True,  # Show an arrow pointing to the data point
        arrowhead=1,  # Style of the arrowhead
        ax=0,  # Horizontal offset of the arrow
        ay=-40  # Vertical offset of the arrow
        
    )

    # Display the chart
    st.plotly_chart(fig_months, use_container_width=True)
else:
    st.warning("No data available for order distribution over months.")

st.markdown("""
Orders grew steadily from late 2016 to late 2017, peaking at approximately **8,000 orders in November 2017** due to festive and holiday demand. However, there was a sharp decline starting August 2018, with a significant drop by September 2018.
""")


#Step 17: Fetch peak order times by hour
peak_order_times_df = fetch_peak_order_times()

# Display Peak Order Times Visualization
st.subheader("Hourly Order Distribution")
if not peak_order_times_df.empty:
    # Create line chart for peak order times
    fig_peak_times = px.line(
        peak_order_times_df,
        x='OrderHour',
        y='OrderCount',
        labels={'OrderHour': 'Hour of the Day', 'OrderCount': 'Number of Orders'},
        markers=True
    )
    
    # Customize x-axis to display all 24 hours
    fig_peak_times.update_xaxes(
        tickvals=list(range(24)),  # Show all 24 hours
        ticktext=[f"{h}" for h in range(24)],  # Format ticks as "0", "1", ..., "23"
        tickangle=0,  # Rotate labels to be vertical (straight upright)
        showgrid=False  # Remove grid lines for clarity
    )
    
    # Remove grid lines from y-axis
    fig_peak_times.update_yaxes(showgrid=False)
    
    # Display the chart
    st.plotly_chart(fig_peak_times, use_container_width=True)
else:
    st.warning("No data available for peak order times.")


st.markdown("""
Orders are minimal during early morning hours (0–5 AM) as most customers are asleep. Activity spikes sharply from 8 AM onwards, peaking between 11 AM and 4 PM with approx 6,500 orders, driven by lunch-hour shopping and convenience. Orders gradually decline after 9 PM as customers wind down for the day.
""")

#Step 18: Fetch total orders by season
total_orders_by_season_df = fetch_total_orders_by_season()

# Display Total Orders by Season Visualization
st.subheader("Seasonal Analysis")
if not total_orders_by_season_df.empty:
    # Create bar chart for total orders by season
    fig_seasons = px.bar(
        total_orders_by_season_df,
        x='Season',
        y='TotalOrders',
        labels={'Season': 'Season', 'TotalOrders': 'Number of Orders'},
        color='TotalOrders',
        color_continuous_scale='Plasma'
    )
    # Ensure x-axis labels are straight
    fig_seasons.update_xaxes(tickangle=0)  # Set tick angle to 0 for straight labels
    
    # Display the chart
    st.plotly_chart(fig_seasons, use_container_width=True)

else:
    st.warning("No data available for total orders by season.")

st.markdown("""
   Orders peak during major holidays, with Summer showing the highest activity , likely driven by events like Independence Day. Spring also exhibits significant activity, followed by Winter, while Fall has the lowest order counts. This seasonal trend suggests that warmer seasons and holiday periods contribute to increased demand
""")

# Step 19: Fetch average delivery delay days by feedback score
st.markdown('<div id="customer-satisfaction" style="padding-top: 60px; margin-top: -60px;"></div>', unsafe_allow_html=True)
avg_delivery_delay_df = fetch_avg_delivery_delay_by_feedback()

# Main Heading for Correlation Analysis
st.header("Correlation Analysis")

# Subheading for Feedback Score vs. Delivery Delay
st.subheader("Feedback Score vs. Delivery Delay Correlation")


if not avg_delivery_delay_df.empty:
    # Create an area chart using Plotly
    fig = px.area(
        avg_delivery_delay_df,
        x='FeedbackScore',
        y='AvgDeliveryDelayDays',
        # title="Average Delivery Delay Days vs. Feedback Score",
        labels={'FeedbackScore': 'Feedback Score', 'AvgDeliveryDelayDays': 'Average Delivery Delay (Days)'},
        color_discrete_sequence=['skyblue'],
        markers=True
    )
    
    # Customize layout
    fig.update_xaxes(tickangle=0)  # Display x-axis labels upright
    fig.update_layout(
        margin=dict(l=20, r=20, t=40, b=40),  # Adjust margins
        xaxis_title="Feedback Score",  # X-axis label
        yaxis_title="Average Delivery Delay (Days)",  # Y-axis label
        showlegend=False,  # Hide legend since it's not needed here
        
    )
    
    # Render the plot in Streamlit
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No data available for average delivery delay by feedback score.")

st.markdown("""
    Higher feedback scores correlate with shorter delivery delays, indicating that timely deliveries significantly impact customer satisfaction.
    """)

#Step 20: Fetch logistics metrics
st.markdown('<div id="logistics-performance" style="padding-top: 60px; margin-top: -60px;"></div>', unsafe_allow_html=True)
logistics_metrics_df = fetch_logistics_metrics()
st.header("Logistics Analysis")

if not logistics_metrics_df.empty:
    # Display raw data
    # st.subheader("Logistics Metrics Table")
    # st.dataframe(logistics_metrics_df)
    st.dataframe(logistics_metrics_df.style.hide_index())

else:
    st.warning("No data available for logistics analysis.")

# Logistics Analysis Insights
st.markdown("""
- **Heaviest Traffic Routes**: 
  - Banten to Banten handles nearly **6,900 orders**, requiring optimized resource allocation.
  
- **Longest Delivery Delays**: 
  - Banten to Jawa Barat averages over **8 days** in delays, indicating bottlenecks needing attention.
  
- **Late Deliveries**: 
  - Approximately **334 orders** were late on the Banten to Banten route, impacting customer trust.

- **Impact of Delays on Satisfaction**: 
  - Longer delays (e.g., Banten to Jawa Barat) correlate with lower satisfaction scores, emphasizing the need to reduce delivery times.

- **Average Shipping Times**: 
  - Deliveries to DKI Jakarta take nearly **10 days** on average, while shipments to Jawa Timur and Jawa Tengah take around **8–9 days**. Faster shipping processes in these regions could enhance competitiveness.
""")

   
# Step 21: Fetch top 20 states with the highest purchase frequency
st.markdown('<div id="geographic-insights" style="padding-top: 60px; margin-top: -60px;"></div>', unsafe_allow_html=True)
st.header("Geographic Analysis")
st.subheader("States with the Highest Purchase Frequency in Indonesia")

# Data for the top 20 states and their purchase frequencies
data = {
    'State': [
        'Banten', 'Jawa Barat', 'DKI Jakarta', 'Jawa Tengah', 'Jawa Timur',
        'Sumatera Utara', 'Sulawesi Selatan', 'Sumatera Selatan', 'Sumatera Barat', 'Papua',
        'DI Yogyakarta', 'Kalimantan Timur', 'Lampung', 'Kalimantan Barat', 'Riau',
        'Kalimantan Selatan', 'Bali', 'Nusa Tenggara Timur', 'Sulawesi Utara', 'Jambi'
    ],
    'PurchaseFrequency': [
        22200, 13368, 13057, 8896, 8756,
        4085, 2472, 2208, 1963, 1892,
        1824, 1759, 1722, 1640, 1617,
        1584, 1460, 1309, 1271, 1152
    ],
    'Latitude': [
        -6.4025, -6.9175, -6.2000, -7.4167, -7.2504,
        3.5914, -5.1354, -2.9911, -0.9478, -4.1941,
        -7.7956, -0.4842, -5.4254, -0.0375, 0.5201,
        -3.3194, -8.4095, -8.6705, 0.7913, -1.6077
    ],
    'Longitude': [
        106.0667, 107.6186, 106.8166, 110.4167, 112.7500,
        98.6709, 119.4238, 104.7459, 100.3671, 138.1411,
        110.3708, 116.8764, 105.2669, 111.4425, 101.4482,
        115.1889, 115.1889, 121.1521, 124.8478, 103.6163
    ]
}

# Create a DataFrame from the data
df = pd.DataFrame(data)

# Create an interactive map using OpenStreetMap
fig_map = px.scatter_mapbox(
    df,
    lat="Latitude",
    lon="Longitude",
    size="PurchaseFrequency",  
    color="PurchaseFrequency",  
    hover_name="State",        
    zoom=4,                 
    mapbox_style="open-street-map",  
    color_continuous_scale="Plasma", 
    size_max=45         
)

# Customize the map layout
fig_map.update_layout(
    margin={"r": 0, "t": 30, "l": 0, "b": 0},
    height=600,
    width=900
)

fig_map.update_traces(showlegend=False)

# Display the map in Streamlit
st.plotly_chart(fig_map, use_container_width=True)

st.markdown("""
 **Banten** leads with the highest purchase frequency, followed by **Jawa Barat**, **DKI Jakarta**, and **Jawa Tengah**. Remote regions like **Papua** and **Nusa Tenggara Timur** show lower purchase frequencies, likely due to smaller populations or limited market penetration.
""")


# Step 22: Display Business Recommendations
st.markdown('<div id="business-recommendations" style="padding-top: 60px; margin-top: -60px;"></div>', unsafe_allow_html=True)
st.header("Business Recommendations")

# 1. Optimize Payment Method Adoption
st.subheader("Optimize Payment Method Adoption")
st.markdown("""
- Promote **Blipay** and **Voucher** in underutilized regions and categories.
- Offer incentives like discounts or cashback for alternative payment methods.
- Focus on high-demand states: **Banten**, **Jawa Barat**, **DKI Jakarta**.
""")

# 2. Target High-Demand Regions
st.subheader("Target High-Demand Regions")
st.markdown("""
- Focus marketing campaigns in **Banten**, **Jawa Barat**, **DKI Jakarta**, and **Jawa Tengah**.
- Tailor promotions to popular categories like **Bed Bath Table** and **Health Beauty**.
""")

# 3. Revitalize Underperforming Categories
st.subheader("Revitalize Underperforming Categories")
st.markdown("""
- Launch targeted promotions for low-demand categories (**Toys**, **Telephony**).
- Analyze customer feedback to address pain points in these categories.
""")

# 4. Enhance Logistics Efficiency
st.subheader("Enhance Logistics Efficiency")
st.markdown("""
- Allocate more resources (vehicles, staff) to high-traffic routes like **Banten to Banten**.
- Investigate delays on routes like **Banten to Jawa Barat** and implement improvements.
""")

# 5. Expand Market Penetration in Remote Regions
st.subheader("Expand Market Penetration in Remote Regions")
st.markdown("""
- Invest in logistics and marketing efforts in remote areas like **Papua** and **Nusa Tenggara Timur**.
- Offer region-specific promotions or loyalty programs.
""")

# 6. Leverage Seasonal Trends
st.subheader("Leverage Seasonal Trends")
st.markdown("""
- Plan seasonal campaigns around holidays (e.g., Christmas, Easter).
- Introduce time-sensitive promotions during peak hours (e.g., 11 AM–4 PM).
""")

# 7. Improve Customer Satisfaction Through Timely Deliveries
st.subheader("Improve Customer Satisfaction")
st.markdown("""
- Prioritize reducing delivery delays in regions like **DKI Jakarta**, **Jawa Timur**, and **Jawa Tengah**.
- Implement real-time tracking and proactive communication for order updates.
""")

# 8. Personalize Customer Experiences
st.subheader("Personalize Customer Experiences")
st.markdown("""
- Offer **Credit Card-exclusive discounts** for popular categories.
- Provide **Blipay cashback** for Electronics purchases.
- Develop loyalty programs based on payment methods and purchase history.
""")

# 9. Monitor and Address Declining Trends
st.subheader("Monitor and Address Declining Trends")
st.markdown("""
- Investigate the decline in orders starting August 2018.
- Refine strategies to improve product offerings, customer service, and retention.
""")