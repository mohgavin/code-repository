
source /root/anaconda3/etc/profile.d/conda.sh && \
conda activate base && \
cd /var/opt/common5/script/cm
python "/var/opt/common5/script/cm/sftp_sync_status.py" >> /var/opt/common5/log/cm_collect.log