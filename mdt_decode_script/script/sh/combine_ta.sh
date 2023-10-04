
source /root/anaconda3/etc/profile.d/conda.sh && \
conda activate base && \
python "/var/opt/common5/script/ta_combine_xl.py" >> /var/opt/common5/log/ta_combine_xl.txt
python "/var/opt/common5/script/mdt_copy_xl.py" >> /var/opt/common5/log/mdt_combine.txt
python "/var/opt/common5/script/swapsector.py" >> /var/opt/common5/log/swapsector.txt
python "/var/opt/common5/script/filter_bm_mdt.py" >> /var/opt/common5/log/mdt_bm.txt
python "/var/opt/common5/script/filter_mdt.py" >> /var/opt/common5/log/filter_mdt.txt
