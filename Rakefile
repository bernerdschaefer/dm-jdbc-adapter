require 'rubygems'
require 'spec/rake/spectask'

# Specs
task :default => :spec
Spec::Rake::SpecTask.new("spec") do |t|
  t.spec_opts << "--colour"
  t.spec_files = Dir["spec/**/*_spec.rb"]
end
