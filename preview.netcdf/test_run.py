import netcdfutils
import os

sample_file = 'adaptor.mars.internal-1696624738.5653653-18904-2-b0069ad2-7c40-4404-acd9-d7cf76870e2a.nc'

path_to_file = os.path.join(os.getcwd(), sample_file)

png_previews = netcdfutils.generate_maps_for_file(path_to_file=path_to_file)

print('generated previews')