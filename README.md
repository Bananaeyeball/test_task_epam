Task
=====

The code presented in the task is running in a cronjob every 1 hour as the following task:

```ruby
namespace :mraba do
  task :import do
    CsvExporter.transfer_and_import
  end
end
```

It is connecting via FTP to the "Mraba" service and importing list of transactions from there. The code have the following issues:

* runs very slow
* when occasionally swallow errors
* tests for the code are unreliable and incomplete
* had for new team members to get around it
* not following coding standards

__Instalation__

```
bundle
```

__Test running__
```
rake
```
