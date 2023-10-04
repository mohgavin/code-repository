
source /root/anaconda3/etc/profile.d/conda.sh && \
conda activate base && \
cd /var/opt/common5/script/eniq
python "/var/opt/common5/script/eniq/eniq_lte_hourly.py" >> /var/opt/common5/log/eniq_lte_hourly.log
python "/var/opt/common5/script/eniq/pd_kpi_lte_hourly.py" >> /var/opt/common5/log/pd_kpi_lte_hourly.log
