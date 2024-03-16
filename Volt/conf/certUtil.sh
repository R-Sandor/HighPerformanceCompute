docker cp volt-es01-1://usr/share/elasticsearch/config/certs/ca/ca.crt .
keytool -importcert -noprompt -trustcacerts -file ca.crt -keystore \
	volt_store.jks -alias volt -storepass changeme
