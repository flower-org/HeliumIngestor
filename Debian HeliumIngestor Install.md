On Debian 11


## 1. Static network setup

It's possible to manually set network at install time, or else:

The static IP address has been set up using the `/etc/network/interfaces` file on Debian 11.
```
	auto INTERFACE_NAME
	iface INTERFACE_NAME inet static

	address 192.168.1.1

	netmask 255.255.255.0

	gateway 192.168.1.1

	dns-nameservers 8.8.8.8 8.8.4.4
```

restart machine for network

## 2. SSH server with PKI / key pair 

Quote:  
https://www.digitalocean.com/community/tutorials/how-to-configure-ssh-key-based-authentication-on-a-linux-server

### Copying an SSH Public Key to Your Server

The content of your  `id_rsa.pub`  file will have to be added to a file at  `~/.ssh/authorized_keys`  on your remote machine somehow.

Make sure the `~/.ssh` directory is created:  
`mkdir -p ~/.ssh`

Create or modify the `authorized_keys` file:  
`echo PUBLIC_KEY_STR >> ~/.ssh/authorized_keys`

In the above command, substitute the `PUBLIC_KEY_STR` with the output from the `cat ~/.ssh/id_rsa.pub` command that you executed on your local system. It should start with `ssh-rsa AAAA...` or similar.

### Quick method

`cat id_rsa.pub | ssh username@remote_host "mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys"`

### Disabling Password Authentication on your Server

- `sudo nano /etc/ssh/sshd_config`
- update contents: `PasswordAuthentication no`
- `sudo systemctl restart ssh`

### Log In with PKCS#11 / E-Token
- `ssh -I /usr/lib/libeToken.so admin@34.203.194.238`  
- `ssh -I /usr/local/lib/libeToken.dylib IP_ADDR`  


- `sftp -o "SmartcardDevice=/usr/local/lib/libeToken.dylib" ubuntu@ubuntu-server.aws.com`  
- `sftp -o "SmartcardDevice=/usr/lib/libeToken.so" user@example.com`


## 3. User setup - `sudo`

`apt install sudo`

Apparently, the following 2 things need to be done:
1) Added user john to sudo group:  
   `sudo usermod -aG sudo john`

2) Added the following line to `/etc/sudoers`  
   `john    ALL=(ALL:ALL) ALL`

3) Passwordless sudo: (?bad idea?):  
   if desired, change the line in `/etc/sudoers`
```
%sudo   ALL=(ALL:ALL) ALL
->
%sudo   ALL=(ALL:ALL) NOPASSWD: ALL
```

4) Update root password to gibberish and don't save - to prevent direct login as root
```
$ su
$ openssl rand -base64 48
<PASSWORD>

$ passwd - update password
```

5) Update non-root user password to gibberish and don't save with passwordless sudo  
   If you're using passwordless sudo (don't use passwordless sudo) AND you're using PKI for login, same can be done to your **non-root user** password.

6) Disable local login (do for all `tty0` - `ttyN` if needed)
```
sudo systemctl disable getty@tty0.service
sudo systemctl stop getty@tty0.service
```

### TODO: $PATH for user and root



## 4. Add more OS CDs for apt if needed
`apt-cdrom add`

## 5. Setting date and time

The following command sets the date to 7 April 2020 and time to 11:20:15.  
It sets local timezone.
```
$timedatectl set-ntp false
$timedatectl set-time '2020-04-07 11:20:15'
```

## 6. For laptops - no sleep on lid close, turn screen off

### 6.1 no sleep on lid close

`sudo nano /etc/systemd/logind.conf`

Modify the lid switch settings: In the file, look for the lines:
```
#HandleLidSwitch=suspend
#HandleLidSwitchExternalPower=suspend
#HandleLidSwitchDocked=suspend
```

Uncomment these lines by removing the # at the beginning, and change their values to ignore:
```
HandleLidSwitch=ignore
HandleLidSwitchExternalPower=ignore
HandleLidSwitchDocked=ignore
```

Save the changes and exit the editor.
Restart the systemd-logind service: Apply the changes by restarting the logind service. Run:

`sudo systemctl restart systemd-logind`
After these steps, your laptop should no longer hibernate when you close the lid.

### 6.2 turn screen off

`sudo apt install vbetool`  
`sudo vbetool dpms off`


## 7. Install utilities

`sudo apt install mc`

## 8. Install ffmpeg

`sudo apt install ffmpeg`

## 9. Install java 17
- Install OpenJDK JRE
  - `sudo apt install openjdk-17-jre` (will cause error)
  - `sudo apt --fix-broken install`
  - `sudo apt install openjdk-17-jre` (do it again)

To check your apt cache:
```
$ apt-cache search openjdk | grep 17
...
openjdk-17-jdk - OpenJDK Development Kit (JDK)
openjdk-17-jdk-headless - OpenJDK Development Kit (JDK) (headless)
openjdk-17-jre - OpenJDK Java runtime, using Hotspot JIT
openjdk-17-jre-headless - OpenJDK Java runtime, using Hotspot JIT (headless)
openjdk-17-jre-zero - Alternative JVM for OpenJDK, using Zero
openjdk-17-source - OpenJDK Development Kit (JDK) source files
...
```

## 10. TODO: iptables
    Leave 22 and 8080 open (mb 8443 w/SSL?)

## 11. TODO: Helium service setup: run Helium Ingestor on startup
use `sftp` to copy files

### If debug-running directly (no ssh), don't logout, nohup jobs will stop.
instead, do

`apt install vlock`  
`vlock`

also to turn screen off,  
`apt install vbetool`  
`sudo vbetool dpms off`

To regain control of the console on pressing Enter key, I suggest  
`sudo sh -c 'vbetool dpms off; read ans; vbetool dpms on'`

lock.sh
```
/sbin/vbetool dpms off
read ans
/sbin/vbetool dpms on
vlock
```
