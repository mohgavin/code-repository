
source /root/anaconda3/etc/profile.d/conda.sh && \
conda activate base && \
python "/var/opt/common5/script/sftp_get_ctr_hourly_pycrate_ctr1_enm6.py" >> /var/opt/common5/log/log_ftp_ctrmdt_enm6.txt
