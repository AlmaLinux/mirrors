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
    libvirt.graphics_type = "spice"
    libvirt.channel :type => 'spicevmc', :target_name => 'com.redhat.spice.0', :target_type => 'virtio'
    libvirt.redirdev :type => "spicevmc"
    libvirt.uri = 'qemu:///system'
  end

  config.vm.define "mirrors-service" do |i|
    i.vm.hostname = "alma-8-mirrors-service"
    i.vm.network "private_network", ip: "10.0.0.10"
  end

  config.vm.define "mirror1" do |i|
    i.vm.hostname = "alma-8-mirror-1"
    i.vm.network "private_network", ip: "10.0.0.11"
  end

end
