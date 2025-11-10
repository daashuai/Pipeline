local dap = require('dap')
dap.adapters.python = {
    type = 'executable',
    command = 'python',
    args = {'-m', 'debugpy.adapter'}
}

dap.configurations.python = {
    {
      type = 'python';
      request = 'launch';
      name = "Launch file";
      program = "${file}";
    },
    {
      type = 'python';
      request = 'launch';
      name = "Hog similarity";
      program = "${file}";
      args = {'./sample_image', './source_image'}
    },
    {
      type = 'python';
      request = 'launch';
      name = "Distribution similarity";
      program = "${file}";
      args = {'./sample_align', './source_align'}
    },
    {
      type = 'python';
      request = 'launch';
      name = "Find Peaks";
      program = "${file}";
      args = {'/home/ludashuai/Project/WaterQualityMonitoring/code-3d/sample_gmm/s091306胜景路西四路雨水涵.TXT'}
    },
    {
      type = 'python';
      request = 'launch';
      name = "Plot with file";
      program = "${file}";
      args = {'./sample_align/十万沟入锈针河.TXT'}
    },
}
