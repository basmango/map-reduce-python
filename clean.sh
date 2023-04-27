sudo rm -rf blocking/filestore/*
sudo rm -rf non-blocking/filestore/*
sudo lsof -i :50000-60000 | grep "python3" | awk '{print $2}' | xargs sudo kill 

# kill all procecss running on ports 9000 to 9005
sudo lsof -i :9000-9005 | grep "python3" | awk '{print $2}' | xargs sudo kill
