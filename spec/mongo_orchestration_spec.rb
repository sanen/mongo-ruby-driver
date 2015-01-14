require 'spec_helper'

describe MongoOrchestration, if: mongo_orchestration_available? do

  context 'when creating a standalone' do

    context 'when the mongo orchestration service is available' do

      before do
        initialize_mo!
      end

      after do
        stop_mo!
      end

      it 'sets up a standalone object with an id' do
        expect(@mo.id).to_not be_nil
      end

      it 'sets up a standalone object with a client' do
        expect(@mo.client).to_not be_nil
      end

      it 'sets the correct cluster topology' do
        expect(@mo.client.cluster.topology).to eq(Mongo::Cluster::Topology::Standalone)
      end

      it 'sets the path' do
        expect(@mo.path).to eq(MongoOrchestration::DEFAULT_BASE_URI)
      end
    end

    context 'when the mongo orchestration service is not available' do

      it 'raises an error' do
        expect do
          initialize_mo!(path: 'http://localhost:1')
        end.to raise_exception(MongoOrchestration::ServiceNotAvailable)
      end
    end
  end

  context 'when stopping a standalone' do

    before do
      initialize_mo!
      stop_mo!
    end

    it 'stops the standalone' do
      expect(@mo.alive?).to eq(false)
    end
  end

  context 'when making a direct request' do

    before do
      initialize_mo!
    end

    after do
      stop_mo!
    end

    it 'returns the response' do
      expect(@mo.request('GET', 'servers').class).to be(HTTParty::Response)
    end
  end
end