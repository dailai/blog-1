





关于state选项的使用

https://serverfault.com/questions/618164/block-outgoing-connections-on-rhel7-centos7-with-firewalld





iptables的原理

http://www.zsythink.net/archives/1199



firewalld的基本使用

https://www.digitalocean.com/community/tutorials/how-to-set-up-a-firewall-using-firewalld-on-centos-7

https://www.linode.com/docs/security/firewalls/introduction-to-firewalld-on-centos/







iptables版本

机器A   机器B  机器C

机器B只允许来自机器A的ssh请求

机器B只能访问机器C的10000端口



入网限制

```
iptables -A INPUT -p tcp -s 123.45.6.0/32 --dport 22 -m state --state NEW,ESTABLISHED -j ACCEPT
iptables -I INPUT -p tcp --dport 22 -j DROP


iptables -A OUTPUT -p tcp --sport 22 -m state --state ESTABLISHED -j ACCEPT
```



出网限制

```shell
iptables -A OUTPUT -p tcp -d 123.45.6.0/32 --dport 1000 -m state --state NEW,ESTABLISHED -j ACCEPT

iptables -A OUTPUT -j DROP

```



firewall版本

```shell
firewall-cmd --permanent --add-rich-rule 'rule family="ipv4" source address="123.123.123.123" port port=3389 protocol=tcp accept'

firewall-cmd --permanent --direct --add-rule ipv4 filter OUTPUT 0 -p tcp --sport 22 -m state --state ESTABLISHED -j ACCEPT

firewall-cmd --permanent --direct --add-rule ipv4 filter OUTPUT 1 -p tcp -d 456.456.456.456/32 --dport 1000 -m state --state NEW,ESTABLISHED -j ACCEPT

firewall-cmd --permanent --direct --add-rule ipv4 filter OUTPUT 2 -j DROP

```







