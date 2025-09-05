sudo docker rm -f prod-rally
sudo chown -R ubuntu:ubuntu /home/vino/producer-rally/*
sudo chmod 777 /home/vino/producer-rally/prod-rally.log
sudo docker compose -f /home/vino/producer-rally/compose.yml -p rally up