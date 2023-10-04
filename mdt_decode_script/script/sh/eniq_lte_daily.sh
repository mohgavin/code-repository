
source /root/anaconda3/etc/profile.d/conda.sh && \
conda activate base && \
cd /var/opt/common5/script/eniq
python "/var/opt/common5/script/eniq/eniq_lte_daily.py" >> /var/opt/common5/log/eniq_lte_daily.log
python "/var/opt/common5/script/eniq/pd_kpi_lte_daily.py" >> /var/opt/common5/log/pd_kpi_lte_daily.log
python "/var/opt/common5/script/eniq/eniq_asm.py" >> /var/opt/common5/log/eniq_asm.log
