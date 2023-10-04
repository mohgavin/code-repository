
source /root/anaconda3/etc/profile.d/conda.sh && \
conda activate base && \
python "/var/opt/common5/script/cleanUp_old_ctr.py" >> /var/opt/common5/log/log_cleanup_ctr_mdt.txt