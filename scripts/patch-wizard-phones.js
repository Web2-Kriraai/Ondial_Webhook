const fs = require('fs');
const p =
  'C:/Users/ADMIN/Documents/GitHub/Ondial/app/dashboard/campaigns/campaignsform/page.js';
const lines = fs.readFileSync(p, 'utf8').split(/\r?\n/);

function findLine(substr, from = 0) {
  for (let i = from; i < lines.length; i++) {
    if (lines[i].includes(substr)) return i;
  }
  return -1;
}

// Block 1: Auto-add +91 sheets customFields
let s1 = findLine('Auto-add +91 to 10-digit mobile numbers in Google Sheets data');
if (s1 >= 0) {
  const e1 = findLine('return processedRecord;', s1);
  if (e1 < 0) throw new Error('e1');
  const repl1 = [
    '                // Market-aware phone normalize (IN → +91; foreign → destination ISO / E.164)',
    '                Object.keys(processedRecord).forEach((key) => {',
    '                  const value = processedRecord[key];',
    '                  if (value && typeof value === \'string\' && isPhoneLikeHeader(key)) {',
    '                    const normalized = normalizeLeadPhoneInputForMarket(',
    '                      profileUser?.market,',
    '                      value,',
    '                      { campaignDestinationIso: effectiveLeadPhoneDestinationIso }',
    '                    );',
    '                    if (normalized) processedRecord[key] = normalized;',
    '                  }',
    '                });',
    '',
  ];
  lines.splice(s1, e1 - s1, ...repl1);
  console.log('replaced block1', s1, e1);
} else {
  console.log('block1 already gone');
}

// Block 2: Auto-add +91 phone field
let s2 = findLine('Auto-add +91 to phone field with validation');
if (s2 >= 0) {
  // find next blank line then return processedRecord
  let e2 = s2;
  while (e2 < lines.length && !(lines[e2].trim() === '' && lines[e2 + 1]?.includes('return processedRecord'))) {
    e2++;
  }
  if (e2 >= lines.length) throw new Error('e2');
  const repl2 = [
    '                if (processedRecord.phone && typeof processedRecord.phone === \'string\') {',
    '                  const normalized = normalizeLeadPhoneInputForMarket(',
    '                    profileUser?.market,',
    '                    processedRecord.phone,',
    '                    { campaignDestinationIso: effectiveLeadPhoneDestinationIso }',
    '                  );',
    '                  if (normalized) processedRecord.phone = normalized;',
    '                }',
  ];
  lines.splice(s2, e2 - s2, ...repl2);
  console.log('replaced block2', s2, e2);
} else {
  console.log('block2 already gone');
}

// Zoho formatPhoneNumbers
let z = findLine('Format phone numbers with +91 prefix for Zoho CRM data');
if (z >= 0) {
  let ze = findLine('};', z);
  // find the closing of formatPhoneNumbers — look for "};" after "return formattedRecord"
  for (let i = z; i < lines.length; i++) {
    if (lines[i].includes('return formattedRecord;')) {
      // next lines until `};`
      for (let j = i; j < i + 10; j++) {
        if (lines[j].trim() === '};') {
          ze = j;
          break;
        }
      }
      break;
    }
  }
  const replZ = [
    '                        const formatPhoneNumbers = (data) => {',
    '                          return data.map((record) => {',
    '                            const formattedRecord = { ...record };',
    "                            const phoneFields = ['mobile', 'Mobile', 'phone', 'Phone', 'contact', 'Contact', 'Number', 'number', 'NUMBER', 'MOBILE', 'PHONE', 'CONTACT'];",
    '                            phoneFields.forEach((field) => {',
    '                              if (formattedRecord[field]) {',
    '                                const normalized = normalizeLeadPhoneInputForMarket(',
    '                                  profileUser?.market,',
    '                                  formattedRecord[field],',
    '                                  { campaignDestinationIso: effectiveLeadPhoneDestinationIso }',
    '                                );',
    '                                if (normalized) formattedRecord[field] = normalized;',
    '                              }',
    '                            });',
    '                            return formattedRecord;',
    '                          });',
    '                        };',
  ];
  lines.splice(z, ze - z + 1, ...replZ);
  console.log('replaced zoho', z, ze);
}

let out = lines.join('\n');
out = out.replace(
  /record\[field\] && \/\^\\\+91\\d\{10\}\$\/\.test\(record\[field\]\)/g,
  "record[field] && /^\\+[1-9]\\d{7,14}$/.test(String(record[field]).replace(/\\s+/g, ''))"
);

fs.writeFileSync(p, out);
console.log('done');
