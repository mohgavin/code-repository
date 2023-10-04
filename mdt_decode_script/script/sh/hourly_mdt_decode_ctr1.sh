
source /root/anaconda3/etc/profile.d/conda.sh && \
conda activate base && \
python "/var/opt/common5/script/hourly_mdt_pycrate_ctr1.py" >> /var/opt/common5/log/log_parsing_ctrmdt.txt
