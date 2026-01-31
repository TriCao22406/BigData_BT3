import streamlit as st
import pymongo
import pandas as pd
import vaex
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Cấu hình trang Streamlit
st.set_page_config(page_title="Big Data Analysis - MongoDB & Vaex", layout="wide")

st.title("📊 Phân tích dữ liệu Bank Marketing từ MongoDB Atlas")
st.markdown("Bài tập giữa kỳ 3 - Ứng dụng Vaex và Streamlit")

# 2. Hàm tải dữ liệu (Sử dụng Cache để không phải tải lại mỗi lần click chuột)
@st.cache_data
def load_data_from_mongo():
    # Chuỗi kết nối từ đề bài
    connection_string = "mongodb+srv://tricm22406_db_user:EWCbJopYyr0fwvGA@bt3.e4gojd9.mongodb.net/"
    
    try:
        client = pymongo.MongoClient(connection_string)
        db = client["BT3"]
        collection = db["bank_marketing"]
        
        # Lấy dữ liệu (Giới hạn 50,000 dòng demo để load nhanh, thực tế có thể bỏ limit)
        # Lưu ý: Dùng list() để kéo về RAM xử lý bước đầu
        cursor = collection.find().limit(50000)
        data = list(cursor)
        
        if not data:
            return None

        # Xử lý làm phẳng dữ liệu (Flatten Nested JSON)
        # Vì dữ liệu có dạng {'cons': {'price': ...}}, ta cần duỗi ra
        df = pd.json_normalize(data)

        # Xóa cột _id và đổi tên cột có dấu chấm (Vaex không thích dấu chấm trong tên cột)
        if "_id" in df.columns:
            del df["_id"]
        
        df.columns = [c.replace('.', '_') for c in df.columns]
        
        return df
        
    except Exception as e:
        st.error(f"Lỗi kết nối MongoDB: {e}")
        return None

# 3. Main App Logic
with st.spinner('Đang tải dữ liệu từ MongoDB Atlas...'):
    df_pandas = load_data_from_mongo()

if df_pandas is not None:
    # Chuyển đổi sang Vaex DataFrame
    vdf = vaex.from_pandas(df_pandas)
    
    # --- Sidebar: Bộ lọc dữ liệu ---
    st.sidebar.header("🔍 Bộ lọc dữ liệu")
    
    # Lấy danh sách unique jobs để tạo selectbox
    unique_jobs = df_pandas['job'].unique().tolist()
    selected_job = st.sidebar.multiselect("Chọn nghề nghiệp (Job):", unique_jobs, default=unique_jobs[:3])
    
    unique_marital = df_pandas['marital'].unique().tolist()
    selected_marital = st.sidebar.multiselect("Tình trạng hôn nhân:", unique_marital, default=unique_marital)

    # --- Áp dụng bộ lọc với Vaex ---
    # Vaex lọc cực nhanh bằng cách tạo selection mask
    vdf_filtered = vdf
    
    if selected_job:
        # Lọc theo danh sách job đã chọn
        vdf_filtered = vdf_filtered[vdf_filtered['job'].isin(selected_job)]
    
    if selected_marital:
        vdf_filtered = vdf_filtered[vdf_filtered['marital'].isin(selected_marital)]

    # --- Hiển thị Metrics (Chỉ số tổng quan) ---
    col1, col2, col3 = st.columns(3)
    
    # Đếm số dòng sau khi lọc
    count = vdf_filtered.count() 
    # Tính tuổi trung bình
    avg_age = vdf_filtered['age'].mean()
    # Tính thời gian gọi trung bình
    avg_duration = vdf_filtered['duration'].mean()

    col1.metric("Tổng số bản ghi", f"{int(count):,}")
    col2.metric("Độ tuổi trung bình", f"{float(avg_age):.1f}")
    col3.metric("Thời lượng gọi TB (giây)", f"{float(avg_duration):.1f}")

    # --- Hiển thị Dữ liệu ---
    st.subheader("1. Bảng dữ liệu chi tiết (Top 5)")
    # Chuyển 5 dòng đầu của Vaex về Pandas để hiển thị trên Streamlit
    st.dataframe(vdf_filtered.head(5).to_pandas_df())

    # --- Biểu đồ phân tích ---
    st.subheader("2. Trực quan hóa dữ liệu")
    
    c1, c2 = st.columns(2)

    with c1:
        st.write("**Phân phối độ tuổi (Age Distribution)**")
        fig, ax = plt.subplots()
        # Vaex vẽ histogram trực tiếp
        vdf_filtered.viz.histogram(vdf_filtered.age, label='Age', color='skyblue')
        plt.xlabel("Tuổi")
        plt.ylabel("Số lượng")
        st.pyplot(fig)

    with c2:
        st.write("**Mối quan hệ Tuổi & Thời lượng gọi (Heatmap)**")
        st.caption("Sử dụng Vaex Heatmap để xử lý dữ liệu lớn thay vì Scatter plot")
        fig2, ax2 = plt.subplots()
        # Vẽ heatmap: Cực mạnh của Vaex cho Big Data
        vdf_filtered.viz.heatmap(vdf_filtered.age, vdf_filtered.duration, limits='99%')
        plt.xlabel("Tuổi")
        plt.ylabel("Thời lượng (giây)")
        st.pyplot(fig2)

    # Biểu đồ đếm Job (Dùng Pandas kết hợp Seaborn cho đẹp vì số lượng Job ít)
    st.write("**Thống kê theo Nghề nghiệp**")
    job_counts = vdf_filtered['job'].value_counts(progress=False)
    # Chuyển về Pandas Series để vẽ bar chart dễ hơn
    job_counts_pd = pd.Series(job_counts).sort_values(ascending=False)
    
    st.bar_chart(job_counts_pd)

else:
    st.warning("Không có dữ liệu để hiển thị.")
