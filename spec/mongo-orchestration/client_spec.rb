require 'spec_helper'
require 'yaml'

describe Mongo::Client, if: mongo_orchestration_available? do

  context 'when creating a standalone' do

    let(:test_spec) do
      YAML::load(File.open("spec/support/shared/standalone_available.yml"))
    end

    before do
      initialize_mo!(spec: test_spec)
    end

    after do
      stop_mo!
    end

    it 'can successfully do a read' do
      expect do
        @mo.run
      end.to_not raise_exception
    end
  end
end
