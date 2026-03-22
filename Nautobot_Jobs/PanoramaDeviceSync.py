"""Panorama Device Sync Job - imports devices from Palo Alto Panorama into Nautobot."""

import os

from django.contrib.contenttypes.models import ContentType
from django.db import transaction

from nautobot.apps.jobs import Job, ObjectVar, register_jobs
from nautobot.dcim.models import Device, DeviceType, Interface, Manufacturer, Platform, SoftwareVersion
from nautobot.extras.models import Role, Status
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

            try:
                with transaction.atomic():
                    device_type, _ = DeviceType.objects.get_or_create(model=model_name, manufacturer=manufacturer)

                    device, created = Device.objects.update_or_create(
                        serial=serial,
                        defaults={
                            "name": hostname,
                            "device_type": device_type,
                            "role": role,
                            "platform": platform,
                            "status": active if entry.findtext("connected") == "yes" else offline,
                            "location": location,
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

            except Exception as e:
                self.logger.error("Failed to process %s (%s): %s", hostname, serial, e)

        self.logger.info("Panorama device sync complete.")


register_jobs(PanoramaDeviceSync)