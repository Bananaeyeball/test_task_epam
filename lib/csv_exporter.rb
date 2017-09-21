require 'net/sftp'
require 'iconv'
require 'csv'

class CsvExporter
  @sftp_server = Rails.env == 'production' ? 'csv.example.com/endpoint/' : '0.0.0.0:2020'

  cattr_accessor :import_retry_count

  def self.transfer_and_import(send_email = true)
    FileUtils.mkdir_p "#{Rails.root}/private/data/download"

    Net::SFTP.start(@sftp_server, 'some-ftp-user', keys: ['path-to-credentials']) do |sftp|
      sftp_entries = sftp.dir.entries('/data/files/csv').map(&:name).sort
      sftp_entries.each do |entry|
        next unless entry[-4, 4] == '.csv' || sftp_entries.include?("#{entry}.start")

        file_local = "#{Rails.root}/private/data/download/#{entry}"

        handle_file_remote(entry, file_local, sftp)

        result = import(file_local)

        handle_result(entry, result, send_email, file_local)
        break unless result == 'Success'
      end
    end
  end

  def self.handle_file_remote(entry, file_local, sftp)
    file_remote = "/data/files/csv/#{entry}"

    sftp.download!(file_remote, file_local)
    sftp.remove!(file_remote + '.start')
  end

  def self.handle_result(entry, result, send_email, file_local)
    if result == 'Success'
      File.delete(file_local)
    else
      upload_error_file(entry, result)
    end

    send_result(entry, result) if send_email
  end

  def self.send_result(entry, result)
    success_log = ['Successful Import', "Import of the file #{entry} done."]
    error_log =
      ['Import CSV failed', ["Import of the file #{entry} failed with errors:", result].join("\n")]

    send_content = result == 'Success' ? success_log : error_log
    BackendMailer.send_import_feedback(*send_content)
  end

  def self.import(file, validation_only = false)
    @errors = []
    result = begin
               import_file(file, validation_only)
             rescue => e
               { errors: [e.to_s], success: ['data lost'] }
             end

    get_import_result(file, result)
  end

  def self.get_import_result(file, result)
    result =
      if result[:errors].blank?
        'Success'
      else
        "Imported: #{result[:success].join(', ')} Errors: #{result[:errors].join('; ')}"
      end

    Rails.logger.info "CsvExporter#import time: #{Time.now.to_formatted_s(:db)} Imported #{file}: #{result}"

    result
  end

  def self.import_file(file, validation_only = false)
    line = 2
    source_path = "#{Rails.root}/private/upload"
    path_and_name = "#{source_path}/csv/tmp_mraba/DTAUS#{Time.now.strftime('%Y%m%d_%H%M%S')}"

    FileUtils.mkdir_p "#{source_path}/csv"
    FileUtils.mkdir_p "#{source_path}/csv/tmp_mraba"

    dtaus = Mraba::Transaction.define_dtaus('RS',
                                            8_888_888_888,
                                            99_999_999,
                                            'Credit collection')

    success_rows = []
    import_rows =
      CSV.read(file, col_sep: ';', headers: true, skip_blanks: true).map do |r|
        [r.to_hash['ACTIVITY_ID'], r.to_hash]
      end

    import_rows.each do |index, row|
      break unless validate_import_row(row)
      next if index.blank?

      import_file_row_with_error_handling(row, validation_only, dtaus)
      line += 1
      break unless @errors.empty?
      success_rows << row['ACTIVITY_ID']
    end

    if @errors.empty? && !validation_only
      dtaus.add_datei("#{path_and_name}_201_mraba.csv") unless dtaus.is_empty?
    end

    { success: success_rows, errors: @errors }
  end

  def self.validate_import_row(row)
    return true if %w(10 16).include?(row['UMSATZ_KEY'])

    @errors << "#{row['ACTIVITY_ID']}: UMSATZ_KEY #{row['UMSATZ_KEY']} is not allowed"
    false
  end

  def self.import_file_row_with_error_handling(row, validation_only, dtaus)
    result = nil
    self.import_retry_count = 0
    5.times do
      self.import_retry_count += 1
      begin
        result = import_file_row(row, validation_only, dtaus)
        break
      rescue => e
        result = "#{row['ACTIVITY_ID']}: #{e}"
      end
    end

    result == true ? true : @errors << result
  end

  def self.import_file_row(row, validation_only, dtaus)
    import_file_row =
      case transaction_type(row)
      when 'AccountTransfer' then add_account_transfer(row, validation_only)
      when 'BankTransfer' then add_bank_transfer(row, validation_only)
      when 'Lastschrift' then add_dta_row(dtaus, row, validation_only)
      else "#{row['ACTIVITY_ID']}: Transaction type not found"
      end

    import_file_row == true ? true : import_file_row
  end

  def self.transaction_type(row)
    if row['SENDER_BLZ'] == '00000000' && row['RECEIVER_BLZ'] == '00000000'
      'AccountTransfer'
    elsif row['SENDER_BLZ'] == '00000000' && row['UMSATZ_KEY'] == '10'
      'BankTransfer'
    elsif row['RECEIVER_BLZ'] == '70022200' && row['UMSATZ_KEY'] == '16'
      'Lastschrift'
    else
      false
    end
  end

  def self.get_sender(row)
    sender = Account.find_by_account_no(row['SENDER_KONTO'])

      @errors << "#{row['ACTIVITY_ID']}: Account #{row['SENDER_KONTO']} not found" if sender.nil?
    sender
  end

  def self.add_account_transfer(row, validation_only)
    sender = get_sender(row)
    return @errors.last unless sender

    if row['DEPOT_ACTIVITY_ID'].blank?
      account_transfer = sender.credit_account_transfers.build(amount: row['AMOUNT'].to_f,
                                                               subject: import_subject(row),
                                                               receiver_multi: row['RECEIVER_KONTO'])

      account_transfer.date = row['ENTRY_DATE'].to_date
      account_transfer.skip_mobile_tan = true
    else
      account_transfer = sender.credit_account_transfers.find_by_id(row['DEPOT_ACTIVITY_ID'])
      return "#{row['ACTIVITY_ID']}: AccountTransfer not found" if account_transfer.nil?
      if account_transfer.state != 'pending'
        return "#{row['ACTIVITY_ID']}: AccountTransfer state expected 'pending' but was '#{account_transfer.state}'"
      end
      account_transfer.subject = import_subject(row)
    end
    unless account_transfer.try(:valid?)
      return "#{row['ACTIVITY_ID']}: AccountTransfer validation error(s): #{account_transfer.errors.full_messages.join('; ')}"
    end

    unless validation_only
      row['DEPOT_ACTIVITY_ID'].blank? ? account_transfer.save! : account_transfer.complete_transfer!
    end

    true
  end

  def self.add_bank_transfer(row, validation_only)
    sender = get_sender(row)
    return @errors.last unless sender

    bank_transfer = sender.build_transfer(
      amount: row['AMOUNT'].to_f,
      subject: import_subject(row),
      rec_holder: row['RECEIVER_NAME'],
      rec_account_number: row['RECEIVER_KONTO'],
      rec_bank_code: row['RECEIVER_BLZ']
    )

    unless bank_transfer.valid?
      return "#{row['ACTIVITY_ID']}: BankTransfer validation error(s): #{bank_transfer.errors.full_messages.join('; ')}"
    end
    bank_transfer.save! unless validation_only
    true
  end

  def self.add_dta_row(dtaus, row, _validation_only)
    unless dtaus.valid_sender?(row['SENDER_KONTO'], row['SENDER_BLZ'])
      return "#{row['ACTIVITY_ID']}: BLZ/Konto not valid, csv fiile not written"
    end
    holder = Iconv.iconv('ascii//translit',
                         'utf-8',
                         row['SENDER_NAME']).to_s.gsub(/[^\w^\s]/, '')

    dtaus.add_buchung(row['SENDER_KONTO'],
                      row['SENDER_BLZ'], holder,
                      BigDecimal(row['AMOUNT']).abs,
                      import_subject(row))
    true
  end

  def self.import_subject(row)
    (1..14).each_with_object('') do |id, str|
      str << row["DESC#{id}"].to_s if row["DESC#{id}"].present?
    end
  end

  def self.upload_error_file(entry, result)
    FileUtils.mkdir_p "#{Rails.root}/private/data/upload"
    error_file = "#{Rails.root}/private/data/upload/#{entry}"
    File.open(error_file, 'w') do |f|
      f.write(result)
    end
    Net::SFTP.start(@sftp_server, 'some-ftp-user', keys: ['path-to-credentials']) do |sftp|
      sftp.upload!(error_file, "/data/files/batch_processed/#{entry}")
    end
  end

  private_class_method :upload_error_file
end
