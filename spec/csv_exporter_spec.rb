# encoding: utf-8
require 'spec_helper'
require_relative '../lib/csv_exporter'
require_relative 'fixtures/account'
require_relative 'fixtures/mraba'
require_relative 'fixtures/backend_mailer'

# Fakes for outside objects
describe CsvExporter do

  describe '.transfer_and_import(send_email = true)' do
    before(:all) do
      download_folder = "#{Rails.root}/private/data/download"
      FileUtils.mkdir_p download_folder
      FileUtils.cp "#{Rails.root}/spec/fixtures/csv_exporter.csv", "#{download_folder}/mraba.csv"
    end
    after(:all) do
      FileUtils.rm_r "#{Rails.root}/private"
    end

    before(:each) do
      entries = ['mraba.csv', 'mraba.csv.start', 'blubb.csv']
      sftp_mock = double('sftp')
      Net::SFTP.stub(:start).and_yield(sftp_mock)
      sftp_mock.stub_chain(:dir, :entries, :map).and_return(entries)
      sftp_mock.stub(:download!).with('/data/files/csv/mraba.csv', "#{Rails.root}/private/data/download/mraba.csv")
      sftp_mock.stub(:remove!).with('/data/files/csv/mraba.csv.start')
      sftp_mock.stub(:upload!).with("#{Rails.root}/private/data/upload/mraba.csv", '/data/files/batch_processed/mraba.csv').once
    end

    it 'fails transfers and imports mraba csv  ' do
      CsvExporter.should_receive(:upload_error_file).once.and_call_original
      File.should_receive(:open).with("#{Rails.root}/private/data/download/mraba.csv",
                                      {universal_newliner: false,
                                      col_sep: ';',
                                      headers: true,
                                      skip_blanks: true}).once.and_call_original

      File.should_receive(:open).with("#{Rails.root}/private/data/upload/mraba.csv", 'w').once.and_call_original
      CsvExporter.transfer_and_import
    end

    it 'fails transfers and imports mraba csv' do
      file_to_upload = double('file to upload')
      File.should_receive(:open).with("#{Rails.root}/private/data/download/mraba.csv", {universal_newline: false, col_sep: ';', headers: true, skip_blanks: true}).once.and_call_original

      File.should_receive(:open).with("#{Rails.root}/private/data/upload/mraba.csv", 'w').once.and_yield(file_to_upload)
      file_to_upload.should_receive(:write).once
      CsvExporter.should_receive(:upload_error_file).once.and_call_original
      BackendMailer.should_receive(:send_import_feedback).with(
        'Import CSV failed',
        'Import of the file mraba.csv failed with errors:'\
        '\nImported:  Errors: 01: UMSATZ_KEY 06 is not allowed; 01: Transaction type not found'
      )

      CsvExporter.transfer_and_import
    end

    it 'transfers and imports mraba csv' do
      data = {
        'DEPOT_ACTIVITY_ID' => '',
        'AMOUNT' => '5',
        'UMSATZ_KEY' => '10',
        'ENTRY_DATE' => Time.now.strftime('%Y%m%d'),
        'KONTONUMMER' => '000000001',
        'RECEIVER_BLZ' => '00000000',
        'RECEIVER_KONTO' => '000000002',
        'RECEIVER_NAME' => 'Mustermann',
        'SENDER_BLZ' => '00000000',
        'SENDER_KONTO' => '000000003',
        'SENDER_NAME' => 'Mustermann',
        'DESC1' => 'Geld senden'
      }

      CSV.stub_chain(:read, :map).and_return [ ['123', data ] ]

      BackendMailer.should_receive(:send_import_feedback)
      CsvExporter.transfer_and_import.should be_nil
    end
  end


  describe '.import(file, validation_only = false)' do
    it 'handles exception during import' do
      CsvExporter.stub(:import_file).and_raise(RuntimeError)
      CsvExporter.import(nil).should == 'Imported: data lost Errors: RuntimeError'
    end
  end


  describe '.import_file_row_with_error_handling(row, validation_only = false)' do
    before(:each) do
      @errors = []
      @dtaus = double
    end

    it 'successful import' do
      row = {
        'RECEIVER_BLZ' => '00000000',
        'SENDER_BLZ' => '00000000',
        'ACTIVITY_ID' => '1'
      }

      CsvExporter.stub(:add_account_transfer).and_return(true)
      CsvExporter.import_file_row_with_error_handling(row, false, @errors, @dtaus).should == [@errors,@dtaus]
      CsvExporter.import_retry_count.should == 1
    end

    it 'handles exception in row' do
      row = {
        'RECEIVER_BLZ' => '00000000',
        'SENDER_BLZ' => '00000000',
        'ACTIVITY_ID' => '1'
      }

      CsvExporter.stub(:add_account_transfer).and_raise(RuntimeError)
      CsvExporter.import_file_row_with_error_handling(row, false, @errors, @dtaus)[0].should == ['1: RuntimeError']
      CsvExporter.import_retry_count.should == 1
    end
  end


  describe '.transaction_type(row)' do
    it "returns 'AccountTransfer'" do
      row = { 'SENDER_BLZ' => '00000000', 'RECEIVER_BLZ' => '00000000' }
      CsvExporter.transaction_type(row).should == 'AccountTransfer'
    end

    it "returns 'BankTransfer'" do
      row = { 'SENDER_BLZ' => '00000000', 'UMSATZ_KEY' => '10' }
      CsvExporter.transaction_type(row).should == 'BankTransfer'
    end

    it "returns 'Lastschrift'" do
      row = { 'RECEIVER_BLZ' => '70022200', 'UMSATZ_KEY' => '16' }
      CsvExporter.transaction_type(row).should == 'Lastschrift'
    end

    it "returns 'false'" do
      row = { }
      CsvExporter.transaction_type(row).should be false
    end
  end

  describe '.get_sender(row)' do
    before(:each) do
      @account = double
      Account.stub(:find_by_account_no).with('000000001') { @account }
      @row = { 'SENDER_KONTO' => '000000001' }
    end

    it "finds sender via 'SENDER_KONTO' column" do
      CsvExporter.get_sender(@row).should == @account
    end

    it "fails to find sender via 'SENDER_KONTO' column" do
      @account = nil
      CsvExporter.get_sender(@row).should be nil
    end
  end


  describe '.add_account_transfer(row, validation_only)' do
    before(:each) do
      @account = double :account_no => '000000001'
      @row = { 'AMOUNT' => 10,
               'ENTRY_DATE' => Date.today,
               'DESC1' => 'Subject',
               'SENDER_KONTO' => '000000001',
               'RECEIVER_KONTO' => '000000002' }
      Account.stub(:find_by_account_no).with('000000001').and_return @account
      @account_transfer = double :date= => nil, :skip_mobile_tan= => nil, :valid? => nil, :errors => double(:full_messages => []), :save! => true
      @account.stub_chain :credit_account_transfers, build: @account_transfer
    end

    it 'adds account_transfer (DEPOT_ACTIVITY_ID is blank)' do
      @account_transfer.stub valid?: true
      CsvExporter.add_account_transfer(@row, false).should be true
    end

    it 'fails to add a account_transfer (missing attribute)' do
      @row['AMOUNT'] = nil
      CsvExporter.add_account_transfer(@row, false).should be_kind_of(Array)
    end

    it 'fails to add a account_transfer (missing attribute, validation only)' do
      @row['AMOUNT'] = nil
      CsvExporter.add_account_transfer(@row, true).should be_kind_of(Array)
    end

    it 'returns error' do
      Account.stub(:find_by_account_no).with('000000001').and_return nil
      CsvExporter.add_account_transfer({'SENDER_KONTO' => '000000001'}, false).should be == ': Account 000000001 not found'
    end

    context 'DEPOT_ACTIVITY_ID is not blank' do
      before(:each) do
        @account_transfer.stub :id => 1, :state => 'pending', :subject= => nil, :valid? => true, :complete_transfer! => true
        @account.stub_chain :credit_account_transfers, :find_by_id => @account_transfer
      end

      it 'finds and validates account_transfer' do
        @row['DEPOT_ACTIVITY_ID'] = @account_transfer.id

        @account_transfer.should_receive :complete_transfer!

        CsvExporter.add_account_transfer(@row, false).should eq true
      end

      it 'fails to find account transfer' do
        @row['DEPOT_ACTIVITY_ID'] = '12345'
        @account.stub_chain :credit_account_transfers, :find_by_id => nil
        CsvExporter.add_account_transfer(@row, false).should be nil
      end

      it 'finds account transfer, but is not in pending state' do
        @account_transfer.stub :state => 'initialized'
        @row['DEPOT_ACTIVITY_ID'] = @account_transfer.id
        CsvExporter.add_account_transfer(@row, false).should be nil
      end
    end
  end


  describe '.add_bank_transfer(row, validation_only)' do
    before(:each) do
      @row = { 'AMOUNT' => 10,
               'RECEIVER_NAME' => 'Bob Baumeiter',
               'RECEIVER_BLZ' => '2222222',
               'DESC1' => 'Subject',
               'SENDER_KONTO' => '000000001',
               'RECEIVER_KONTO' => '000000002' }
      bank_transfer = double :valid? => true, :save! => true
      @account = double :build_transfer => bank_transfer
    end

    it 'adds bank transfer' do
      Account.stub(:find_by_account_no).with('000000001').and_return @account
      CsvExporter.add_bank_transfer(@row, false).should be true
    end

    it 'fails to add bank transfer' do
      Account.stub(:find_by_account_no).with('000000001').and_return nil
      CsvExporter.add_bank_transfer(@row, false).should eq ': Account 000000001 not found'
    end
  end


  describe '.add_dta_row(dta, row, validation_only)' do
    before(:each) do
      @row = { 'ACTIVITY_ID' => '1',
               'AMOUNT' => '10',
               'RECEIVER_NAME' => 'Bob Baumeiter',
               'RECEIVER_BLZ' => '70022200',
               'DESC1' => 'Subject',
               'SENDER_KONTO' => '0101881952',
               'SENDER_BLZ' => '30020900',
               'SENDER_NAME' => 'Max Müstermänn',
               'RECEIVER_KONTO' => 'NO2' }
      @dtaus = double
    end

    it 'adds dta row' do
      @dtaus.stub(:valid_sender?).with('0101881952', '30020900').and_return true
      @dtaus.should_receive(:add_buchung).with('0101881952', '30020900', 'Max Mustermann', 10, 'Subject')
      CsvExporter.add_dta_row(@dtaus, @row, false)
    end

    it 'fails to adds dta row' do
      @dtaus.stub(:valid_sender?).with('0101881952', '30020900').and_return false
      @dtaus.should_not_receive(:add_buchung)
      CsvExporter.add_dta_row(@dtaus, @row, false).last.should == '1: BLZ/Konto not valid, csv fiile not written'
    end
  end

  describe '.import_subject(row)' do
    before(:each) do
      @row = { 'DESC1' => 'Sub', 'DESC2' => 'ject' }
    end

    it 'returns subject from row' do
      CsvExporter.import_subject(@row).should == 'Subject'
    end
  end
end
