site: https://vpsttt.com/
email: noc@vpsttt.com
sponsor: VPSTTT
sponsor_url: https://vpsttt.com/

address:
  http: http://almalinux.vpsttt.com/
  https: https://almalinux.vpsttt.com/
  rsync: rsync://almalinux.vpsttt.com/almalinux/

geolocation:
  country: VN
  state_province: Khanh Hoa
  city: Nha Trang

- ipv6: true
- bandwidth: 10 Gbps
- upstream: rsync://rsync.repo.almalinux.org/almalinux/
- update_frequency: 2h
