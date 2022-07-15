# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.box = "almalinux/8"
  config.ssh.forward_agent = true
  config.ssh.forward_x11 = true
  config.vm.provider "libvirt" do |libvirt|
    libvirt.cpus = 1
    libvirt.memory = 2048
    libvirt.random_hostname = true
    libvirt.uri = 'qemu:///system'
  end

  config.vm.define "mirrors-service" do |i|
    i.vm.hostname = "alma-8-mirrors-service"
    i.vm.network "private_network", ip: "10.0.0.10"
  end

  config.vm.define "fake-mirror" do |i|
    i.vm.hostname = "alma-8-fake-mirror"
    i.vm.network "private_network", ip: "10.0.0.11"
  end

end
