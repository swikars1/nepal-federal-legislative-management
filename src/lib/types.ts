// Types matching the database schema

export type BillStatus =
  | "registered"
  | "first_reading"
  | "general_discussion"
  | "amendment_window"
  | "committee_review"
  | "clause_voting"
  | "first_house_passed"
  | "second_house"
  | "joint_sitting"
  | "speaker_certification"
  | "assented"
  | "gazette_published"
  | "amendment_or_repeal";

export type BillHouse = "pratinidhi_sabha" | "rastriya_sabha";

export type BillType = "original" | "amendment";

export type BillCategory = "governmental" | "non_governmental";

export type StatusSource =
  | "parliament_scrape"
  | "gazette_scrape"
  | "manual_entry";

export interface Committee {
  id: number;
  nameNp: string | null;
  nameEn: string | null;
  house: BillHouse | null;
  createdAt: Date;
}

export interface BillCommitteeAssignment {
  id: number;
  billId: string;
  committeeId: number;
  assignedDateBs: string | null;
  assignedDateAd: Date | null;
  reportSubmittedDateBs: string | null;
  reportSubmittedDateAd: Date | null;
  createdAt: Date;
  committee?: Committee | null;
}

export interface BillStatusHistory {
  id: string;
  billId: string;
  status: BillStatus;
  rawStatus: string | null;
  source: StatusSource;
  statusDateBs: string | null;
  statusDateAd: Date | null;
  notes: string | null;
  sourceUrl: string | null;
  recordedAt: Date;
}

export interface Bill {
  id: string;
  parliamentId: string | null;
  registrationNo: string;
  year: string;
  session: string | null;
  titleNp: string | null;
  titleEn: string | null;
  presenter: string | null;
  ministry: string | null;
  house: BillHouse | null;
  billType: BillType | null;
  category: BillCategory | null;
  currentStatus: BillStatus | null;
  currentPhase: number | null;
  registeredDateBs: string | null;
  authenticatedDateBs: string | null;
  registeredDateAd: Date | null;
  authenticatedDateAd: Date | null;
  registeredBillUrl: string | null;
  authenticatedBillUrl: string | null;
  parliamentUrl: string | null;
  lastScrapedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

// Bill with full relations (used for detail pages)
export interface BillWithDetails extends Bill {
  statusHistory: BillStatusHistory[];
  committeeAssignments: BillCommitteeAssignment[];
}

export interface BillsResponse {
  data: Bill[];
  meta: {
    total: number;
    count: number;
    limit: number;
    offset: number;
    hasMore: boolean;
    filters: {
      house: string | null;
      status: string | null;
      category: string | null;
      ministry: string | null;
      year: string | null;
      search: string | null;
    };
  };
}

// Helper function to format bill status for display
export function formatBillStatus(status: BillStatus | null): string {
  if (!status) return "—";

  const statusMap: Record<BillStatus, string> = {
    registered: "Registered",
    first_reading: "First Reading",
    general_discussion: "General Discussion",
    amendment_window: "Amendment Window",
    committee_review: "Committee Review",
    clause_voting: "Clause Voting",
    first_house_passed: "First House Passed",
    second_house: "Second House",
    joint_sitting: "Joint Sitting",
    speaker_certification: "Speaker Certification",
    assented: "Assented",
    gazette_published: "Gazette Published",
    amendment_or_repeal: "Amendment/Repeal",
  };

  return statusMap[status];
}

// Helper function to get status color
export function getStatusColor(status: BillStatus | null): string {
  if (!status)
    return "bg-slate-100 text-slate-600 border-slate-300 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-700/50";

  const colorMap: Partial<Record<BillStatus, string>> = {
    registered:
      "bg-blue-100 text-blue-700 border-blue-300 dark:bg-blue-900/50 dark:text-blue-200 dark:border-blue-700/50",
    first_reading:
      "bg-cyan-100 text-cyan-700 border-cyan-300 dark:bg-cyan-900/50 dark:text-cyan-200 dark:border-cyan-700/50",
    general_discussion:
      "bg-teal-100 text-teal-700 border-teal-300 dark:bg-teal-900/50 dark:text-teal-200 dark:border-teal-700/50",
    amendment_window:
      "bg-amber-100 text-amber-700 border-amber-300 dark:bg-amber-900/50 dark:text-amber-200 dark:border-amber-700/50",
    committee_review:
      "bg-purple-100 text-purple-700 border-purple-300 dark:bg-purple-900/50 dark:text-purple-200 dark:border-purple-700/50",
    clause_voting:
      "bg-pink-100 text-pink-700 border-pink-300 dark:bg-pink-900/50 dark:text-pink-200 dark:border-pink-700/50",
    first_house_passed:
      "bg-green-100 text-green-700 border-green-300 dark:bg-green-900/50 dark:text-green-200 dark:border-green-700/50",
    second_house:
      "bg-emerald-100 text-emerald-700 border-emerald-300 dark:bg-emerald-900/50 dark:text-emerald-200 dark:border-emerald-700/50",
    joint_sitting:
      "bg-indigo-100 text-indigo-700 border-indigo-300 dark:bg-indigo-900/50 dark:text-indigo-200 dark:border-indigo-700/50",
    speaker_certification:
      "bg-violet-100 text-violet-700 border-violet-300 dark:bg-violet-900/50 dark:text-violet-200 dark:border-violet-700/50",
    assented:
      "bg-lime-100 text-lime-700 border-lime-300 dark:bg-lime-900/50 dark:text-lime-200 dark:border-lime-700/50",
    gazette_published:
      "bg-green-100 text-green-800 border-green-400 dark:bg-green-700/50 dark:text-green-100 dark:border-green-600/50",
    amendment_or_repeal:
      "bg-orange-100 text-orange-700 border-orange-300 dark:bg-orange-900/50 dark:text-orange-200 dark:border-orange-700/50",
  };

  return (
    colorMap[status] ||
    "bg-slate-100 text-slate-600 border-slate-300 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-700/50"
  );
}

// Helper function to format house
export function formatHouse(house: BillHouse | null): string {
  if (!house) return "—";

  const houseMap: Record<BillHouse, string> = {
    pratinidhi_sabha: "House of Representatives",
    rastriya_sabha: "National Assembly",
  };

  return houseMap[house];
}

export function formatHouseShort(house: BillHouse | null): string {
  if (!house) return "—";

  const houseMap: Record<BillHouse, string> = {
    pratinidhi_sabha: "HoR",
    rastriya_sabha: "NA",
  };

  return houseMap[house];
}

// Helper function to format bill type
export function formatBillType(type: BillType | null): string {
  if (!type) return "—";

  const typeMap: Record<BillType, string> = {
    original: "Original",
    amendment: "Amendment",
  };

  return typeMap[type];
}

// Helper function to format category
export function formatCategory(category: BillCategory | null): string {
  if (!category) return "—";

  const categoryMap: Record<BillCategory, string> = {
    governmental: "Governmental",
    non_governmental: "Non‑governmental",
  };

  return categoryMap[category];
}
