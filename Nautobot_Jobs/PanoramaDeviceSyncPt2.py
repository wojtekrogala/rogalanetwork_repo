"""Panorama Device Sync Job - imports devices from Palo Alto Panorama into Nautobot. Improved by reading device tag"""

import os

from django.contrib.contenttypes.models import ContentType
from django.db import transaction

from nautobot.apps.jobs import Job, ObjectVar, register_jobs
from nautobot.dcim.models import Device, DeviceType, Interface, Manufacturer, Platform, SoftwareVersion, Location
from nautobot.extras.models import Role, Status, Tag
from nautobot.ipam.models import IPAddress, IPAddressToInterface, Namespace, Prefix

from panos.panorama import Panorama


class PanoramaDeviceSync(Job):
    """Sync devices from Panorama"""

    panorama_device = ObjectVar(model=Device, query_params={"role": "Panorama"}, description="Panorama to query")

    class Meta:
        name = "Panorama Device Sync"
        description = "Synchronise devices from Palo Alto Panorama into Nautobot"

    def run(self, panorama_device):
        # Connect to Panorama using credentials from env vars
        pano = Panorama(
            str(panorama_device.primary_ip4.host),
            os.environ.get("NAPALM_PALO_USER"),
            os.environ.get("NAPALM_PALO_PASS"),
        )
        entries = pano.op("show devices all", cmd_xml=True).findall(".//devices/entry")

        # Fetch device tags from Panorama config tree (operational data never populates <tag/>)
        pano.xapi.get(xpath="/config/mgt-config/devices")
        tag_dict = {
            e.get("name"): [m.text for m in e.findall("vsys/entry/tags/member") if m.text]
            for e in pano.xapi.element_root.iter("entry")
            if e.get("name") and e.findall("vsys/entry/tags/member")
        }
        self.logger.info("Loaded Panorama tags for %d devices", len(tag_dict))

        # Shared objects used for every device — fetched once before the loop
        manufacturer, _ = Manufacturer.objects.get_or_create(name="Palo Alto")
        platform, _ = Platform.objects.get_or_create(name="PAN-OS", defaults={"manufacturer": manufacturer})
        role, _ = Role.objects.get_or_create(name="Firewall", defaults={"color": "ff0000"})
        role.content_types.add(ContentType.objects.get_for_model(Device))

        active = Status.objects.get(name="Active")
        offline = Status.objects.get(name="Offline")
        namespace = Namespace.objects.get(name="Global")

        for entry in entries:
            serial = entry.findtext("serial", "").strip()
            hostname = entry.findtext("hostname", "").strip()
            model_name = entry.findtext("model", "").strip()
            ip_addr = entry.findtext("ip-address", "").strip()

            if not serial or not model_name:
                continue

            location = None
            if ip_addr:
                prefix = Prefix.objects.filter(
                    namespace=namespace, network__net_contains=f"{ip_addr}/32"
                ).order_by("-prefix_length").first() 
                if not prefix:
                    self.logger.error("No parent prefix for %s – skipping %s", ip_addr, hostname)
                    continue
                location = prefix.location

            if not location:
                location = panorama_device.location

            # Resolve role/location/tags from Panorama tags; fall back to defaults if not found
            device_role = role
            device_location = location
            nbtags = []
            tag_overrides = 0
            panorama_tags = tag_dict.get(serial, [])
            self.logger.debug("Device %s has Panorama tags: %s", serial, panorama_tags)
            for tag_str in panorama_tags:
                if "__" not in tag_str:
                    continue
                tag_prefix, _, value = tag_str.partition("__")
                if tag_prefix == "role":
                    try:
                        device_role = Role.objects.get(name=value)
                        self.logger.info("Device %s: role set to '%s' from Panorama tag", serial, value)
                        tag_overrides += 1
                    except Role.DoesNotExist:
                        self.logger.warning("Panorama tag role__%s: Role not found in Nautobot – skipping", value)
                elif tag_prefix == "loc":
                    try:
                        device_location = Location.objects.get(name=value)
                        self.logger.info("Device %s: location set to '%s' from Panorama tag", serial, value)
                        tag_overrides += 1
                    except Location.DoesNotExist:
                        self.logger.warning("Panorama tag loc__%s: Location not found in Nautobot – skipping", value)
                elif tag_prefix == "nbtag":
                    nbtags.append(value)

            try:
                with transaction.atomic():
                    device_type, _ = DeviceType.objects.get_or_create(model=model_name, manufacturer=manufacturer)

                    device, created = Device.objects.update_or_create(
                        serial=serial,
                        defaults={
                            "name": hostname,
                            "device_type": device_type,
                            "role": device_role,
                            "platform": platform,
                            "status": active if entry.findtext("connected") == "yes" else offline,
                            "location": device_location,
                        },
                    )
                    self.logger.info("%s device", "Created" if created else "Updated", extra={"object": device})

                    if ip_addr:
                        mgmt, _ = Interface.objects.get_or_create(
                            device=device, name="management", defaults={"type": "other", "status": active, "mgmt_only": True}
                        )
                        ip_obj, _ = IPAddress.objects.get_or_create(
                            address=f"{ip_addr}/32", namespace=namespace, defaults={"status": active}
                        )
                        IPAddressToInterface.objects.get_or_create(ip_address=ip_obj, interface=mgmt)
                        device.primary_ip4 = ip_obj

                    sw_ver = entry.findtext("sw-version", "").strip()
                    if sw_ver:
                        device.software_version, _ = SoftwareVersion.objects.get_or_create(
                            version=sw_ver, platform=platform, defaults={"status": active}
                        )

                    device.validated_save()

                    for value in nbtags:
                        try:
                            device.tags.add(Tag.objects.get(name=value))
                            tag_overrides += 1
                        except Tag.DoesNotExist:
                            self.logger.warning("Panorama tag nbtag__%s: Tag not found in Nautobot – skipping", value)

                    if tag_overrides:
                        self.logger.info("Device %s: %d attribute(s) set from Panorama tags", serial, tag_overrides)

            except Exception as e:
                self.logger.error("Failed to process %s (%s): %s", hostname, serial, e)

        self.logger.info(
            "Panorama device sync complete. %d device(s) had tag-driven attribute overrides.",
            sum(1 for s in tag_dict if any(t.startswith(("role__", "loc__", "nbtag__")) for t in tag_dict[s])),
        )


register_jobs(PanoramaDeviceSync)
